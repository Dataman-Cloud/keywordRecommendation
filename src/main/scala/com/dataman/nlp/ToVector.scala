package com.dataman.nlp

import com.dataman.omega.service.utils.{Configs => C}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
/**
 * Created by ckb on 2015/8/18.
 */
object ToVector {

  val STOPWORD_PATH = C.ldaStopwordPath
  //val VOCAB_PATH = "hdfs://10.3.12.9:9000/users/root/lda/vocab"
  val VOCAB_PATH2 = C.ldaVocabPath
  val VOCAB_PATH = C.ldaVocabPath

  def wordToVector(sc: SparkContext,
                   paths: Seq[String],
                   vocabSize: Int,
                   stopwordFile: String):(RDD[(Long, Vector)])={

    val textRDD: RDD[String] = sc.textFile(paths.mkString(","))
    val stopwords: Set[String] = sc.textFile(stopwordFile).map(_.trim).filter(_.size > 0).distinct.collect.toSet
    val broadcastsw = sc.broadcast(stopwords)
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> text.split(" ").map(_.trim).filter(x => x.size > 1 && !broadcastsw.value.contains(x))
    }                                                     //?????????????????ID
    tokenized.cache()
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()                                    //??????wordcount
    val fullVocabSize = wordCounts.count()                //?????????
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)

    val vocab1 = sc.textFile(VOCAB_PATH2, 10)
    val v = vocab1.map(x => (x.substring(1, x.lastIndexOf(",")) -> (x.substring(x.lastIndexOf(",")+1, x.size - 1).toInt)))
    val vocab = v.collect.toMap

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

    (documents)

  }

  def strToVector(sc: SparkContext,
                  str:String,
                  vocabSize: Int,
                  stopwordFile: String):(RDD[(Long, Vector)])={
    val arr =Array(str)
    val textRDD: RDD[String] = sc.parallelize(arr)
    val stopwords: Set[String] = sc.textFile(stopwordFile).map(_.trim).filter(_.size > 0).distinct.collect.toSet
    val broadcastsw = sc.broadcast(stopwords)
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> text.split(" ").map(_.trim).filter(x => x.size > 1 && !broadcastsw.value.contains(x))
    }                                                     //?????????????????ID
    tokenized.cache()
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()                                    //??????wordcount
    val fullVocabSize = wordCounts.count()                //?????????
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)

    val vocab1  = sc.textFile(VOCAB_PATH2, 10)
    val v = vocab1.map(x => (x.substring(1, x.lastIndexOf(",")) -> (x.substring(x.lastIndexOf(",")+1, x.size - 1).toInt)))
    val vocab = v.collect.toMap

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

    (documents)


  }

  def wordToVector1(sc: SparkContext,
                   paths: Seq[String],
                   vocabSize: Int,
                   stopwordFile: String):(RDD[(Long, Vector)])={

    val textRDD: RDD[String] = sc.textFile(paths.mkString(","))
    val rdd1:RDD[(String, String)] =textRDD.map(x=>{
      val array = x.split(",")
      if (array.length == 2){
        (array(1), array(0))
      } else ("null", "null")
    }).filter(x=>x._2 != null && x._2 != "null")
    //val indexRdd:RDD[Long]=rdd1.map(x=>x(0).toLong)
    //val wordRdd:RDD[String]=rdd1.map(x=>x(1))
    val stopwords: Set[String] = sc.textFile(stopwordFile).map(_.trim).filter(_.size > 0).distinct.collect.toSet
    val broadcastsw = sc.broadcast(stopwords)
    val tokenized: RDD[(Long, IndexedSeq[String])] = rdd1.map(x => (x._1, x._2.toLong)).map { case (text, id) =>
      id -> text.split(" ").map(_.trim).filter(x => x.size > 1 && !broadcastsw.value.contains(x))
    }                                                     //?????????????????ID
    tokenized.cache()
    /*
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> text.split(" ").map(_.trim).filter(x => x.size > 1 && !broadcastsw.value.contains(x))
    }
     //?????????????????ID
    tokenized.cache()
    */
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()                                    //??????wordcount
    val fullVocabSize = wordCounts.count()                //?????????
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)

    val vocab1 = sc.textFile(VOCAB_PATH, 10)
    val v = vocab1.map(x => (x.substring(1, x.lastIndexOf(",")) -> (x.substring(x.lastIndexOf(",")+1, x.size - 1).toInt)))
    val vocab = v.collect.toMap

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

    (documents)

  }
}
