package com.dataman.nlp

import com.dataman.nlp.util.Mysql
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


/**
 * Created by mymac on 15/9/23.
 */
object SaveTopicToMysql {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ldaModel = LocalLDAModel.load(sc, "hdfs://10.3.12.9:9000/model/two")

    val vocab1 = sc.textFile("hdfs://10.3.12.9:9000/users/root/lda/vocab2", 10)
    val v = vocab1.map(x => (x.substring(1, x.lastIndexOf(",")) -> (x.substring(x.lastIndexOf(",")+1, x.size - 1 ).toInt)))
    val vocabArray = v.repartition(1).sortBy(_._2, ascending = true).map(x => x._1).collect

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topicIndices5= ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    val topics5=topicIndices5.map{ case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => vocabArray(term.toInt) }
    }

    val topicRdd=sc.parallelize(topics.map(x=>x.mkString(",")).zipWithIndex,10)
    val topic5Rdd=sc.parallelize(topics5.map(x=>x.mkString(",")).zipWithIndex,10)
    Mysql.batchProcessing(Mysql.sqlTo_topics_10word(topic5Rdd))
    Mysql.batchProcessing(Mysql.saveToSQL10(topicRdd))

  }
}

