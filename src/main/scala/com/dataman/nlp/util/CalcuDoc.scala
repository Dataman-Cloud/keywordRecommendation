package com.dataman.nlp.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by ckb on 2015/8/21.
 */
object CalcuDoc {

  def calJson(
               sc: SparkContext,
               paths: Seq[String],
               vocabSize: Int,
               stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long)={

    val textRDD: RDD[String] = sc.textFile(paths.mkString(","))
    val stopwords: Set[String] = sc.textFile(stopwordFile).map(_.trim).filter(_.size > 0).distinct.collect.toSet
    val broadcastsw = sc.broadcast(stopwords)
    // Split text into words
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> text.split(" ").map(_.trim).filter(x => x.size > 1 && !broadcastsw.value.contains(x))
    }                                                     //分词，并为每篇文章生成ID
    tokenized.cache()                                     //缓存，用于迭代

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()                                    //计数，wordcount
    val fullVocabSize = wordCounts.count()                //分词后单词总数
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }                                                                      // 取前vocabSize个单词
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)  //（（单词，index），取出的单词总数）
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
    }                                                                      //将分词后的文章解析成为vector（id，vector）

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }               //单词数组对应关系（vocabArray(word) == index)

    (documents, vocabArray, selectedTokenCount)                            //单词个数之和
  }

   def main (args: Array[String]) {
    val str1="46654"
    val str2="1232,231,213,12"
   val result = "{"+str1+":"+str2+"}"
     val query1 =s"INSERT INTO recommend_article (recommend_article) VALUES " + "('" + result + "')"

     println(query1)
  }
}
