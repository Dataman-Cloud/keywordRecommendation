package com.dataman.nlp


import com.dataman.nlp.util.{RelMysql,Mysql}
import org.apache.spark.sql.{SQLContext,Row}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel,OnlineLDAOptimizer,EMLDAOptimizer,LDA,LocalLDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable
import java.text.BreakIterator
import java.sql.{DriverManager,PreparedStatement,Connection}
import org.apache.spark.rdd.{JdbcRDD,RDD}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
import com.dataman.nlp.util.RelMysql._
/**
 * Created by ckb on 2015/8/18.
 */



object LocalLdaExample {

  private case class Params(
                             input: Seq[String] = Seq.empty,
                             k: Int = 20,
                             maxIterations: Int = 10,
                             docConcentration: Vector = Vectors.dense(-1) ,
                             topicConcentration: Double = -1,
                             vocabSize: Int = 100000,
                             stopwordFile: String = "",
                             algorithm: String = "em",
                             checkpointDir: Option[String] = None,
                             checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = new Params()
    val parser = new OptionParser[Params]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[Int]("k")
        .text(s"number of topics. default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Double]("docConcentration")
        .text(s"amount of topic smoothing to use (> 1.0) (-1=auto)." +
        s"  default: ${defaultParams.docConcentration}")
        .action((x, c) => c.copy(docConcentration = Vectors.dense(x)))
      opt[Double]("topicConcentration")
        .text(s"amount of term (word) smoothing to use (> 1.0) (-1=auto)." +
        s"  default: ${defaultParams.topicConcentration}")
        .action((x, c) => c.copy(topicConcentration = x))
      opt[Int]("vocabSize")
        .text(s"number of distinct word types to use, chosen by frequency. (-1=all)" +
        s"  default: ${defaultParams.vocabSize}")
        .action((x, c) => c.copy(vocabSize = x))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
        s"  default: ${defaultParams.stopwordFile}")
        .action((x, c) => c.copy(stopwordFile = x))
      opt[String]("algorithm")
        .text(s"inference algorithm to use. em and online are supported." +
        s" default: ${defaultParams.algorithm}")
        .action((x, c) => c.copy(algorithm = x))
      opt[String]("checkpointDir")
        .text(s"Directory for checkpointing intermediate results." +
        s"  Checkpointing helps with recovery and eliminates temporary shuffle files on disk." +
        s"  default: ${defaultParams.checkpointDir}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"Iterations between each checkpoint.  Only used if checkpointDir is set." +
        s" default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora." +
        "  Each text file line should hold 1 document.")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = c.input :+ x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    System.setProperty("spark.akka.frameSize","1024")
    val conf = new SparkConf().setAppName(s"LDAExample with $params")
    val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens) =
      preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
    corpus.cache()
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.size
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()


    val lda = new LDA()

    val optimizer = params.algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      //case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${params.algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setDocConcentration(params.docConcentration)
      .setTopicConcentration(params.topicConcentration)
      .setCheckpointInterval(params.checkpointInterval)
    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"\t Training time: $elapsed sec")

    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
      println(s"\t Training data average log likelihood: $avgLogLikelihood")
      println()
      val documentIndices=distLDAModel.topicDistributions
      /*
      println("save rdd")
      //documentIndices.saveAsTextFile("hdfs://10.3.12.9:9000/test/docToTopic6.txt")
      println("end")
      */
      documentIndices.foreach{case (idNum ,topicDis) =>
        print(s"DocumentID :$idNum")
        topicDis.toArray.foreach(print)
      }
    }

    if(ldaModel.isInstanceOf[LocalLDAModel]) {
      println("This is LocalLDAmodel")
      val localLDAModel = ldaModel.asInstanceOf[LocalLDAModel]
      localLDAModel.save(sc,"hdfs://10.3.12.9:9000/test/ModelMatrix/Matrix1")
      /* try to predict one article
      val list=List("hdfs://10.3.12.9:9000/test/one_art.json")
      val pridictDoc=ToVector.wordToVector(sc,list,params.vocabSize,params.stopwordFile)
      val infererTopic=localLDAModel.topicDistributions(pridictDoc)
      infererTopic.saveAsTextFile("hdfs://10.3.12.9:9000/test/inferTopic.txt")
      */
    }


    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val topicIndices5= ldaModel.describeTopics(maxTermsPerTopic = 5)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    val topics5=topicIndices5.map{ case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => vocabArray(term.toInt) }
    }

    /* print describe topic
    println(s"${params.k} topics:")

    var result = ""

    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      result += s"TOPIC $i\n"

      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
        result += s"$term\t$weight\n"
      }
      println()
    }
    */

    println("isnert db")
    val topicRdd=sc.parallelize(topics.map(x=>x.mkString(",")).zipWithIndex,10)
    val topic5Rdd=sc.parallelize(topics5.map(x=>x.mkString(",")).zipWithIndex,10)
    Mysql.batchProcessing(Mysql.sqlTo_topics_5word(topic5Rdd))
    Mysql.batchProcessing(Mysql.saveToSQL(topicRdd))
    //topicRdd.saveAsTextFile("hdfs://10.3.12.9:9000/test/doc_word/docToTopic5.txt")
    //topicRdd.foreachPartition(RelMysql.insertData)
    println("insert finish")

    /* db
    val schema=StructType(
      StructField("terms",StringType)::
      StructField("id",IntegerType)::Nil
    )
    val insertTopic =topics.map(x=>x.mkString(",")).zipWithIndex.map(item=>Row.apply(item._1,item._2))
    val rddTopic=sc.parallelize(insertTopic,10)
    sqlContext.createDataFrame(rddTopic,schema).write.format("jdbc").options(
      Map(
        "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
      //"dbtable" -> "topics",
      "driver"->"com.mysql.jdbc.Driver"
    )).mode("append").insertInto("topics")
    */
    //sc.parallelize(List(result), 1).saveAsTextFile("hdfs://10.3.12.9:9000/test/out2.txt")
    sc.stop()
  }

  /**
   * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
   * @return (corpus, vocabulary as array, total token count in corpus)
   */
  private def preprocess(
                          sc: SparkContext,
                          paths: Seq[String],
                          vocabSize: Int,
                          stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    // Get dataset of document texts
    // One document per line in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    val textRDD: RDD[String] = sc.textFile(paths.mkString(","))

    val stopwords: Set[String] = sc.textFile(stopwordFile).map(_.trim).filter(_.size > 0).distinct.collect.toSet
    val broadcastsw = sc.broadcast(stopwords)

    // Split text into words
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> text.split(" ").map(_.trim).filter(x => x.length > 1 && !broadcastsw.value.contains(x))
    }                                                     //?????????????????ID
    tokenized.cache()                                     //???棬???????

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()                                    //??????wordcount
    val fullVocabSize = wordCounts.count()                //?????????
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }                                                                      // ??vocabSize??????
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)  //?????????index??????????????????
    }

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }                                                                      //?????????????????vector??id??vector??

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }               //???????????????vocabArray(word) == index)

    (documents, vocabArray, selectedTokenCount)                            //??????????
  }







}
