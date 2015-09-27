package com.dataman.nlp

import com.dataman.nlp.knn.knnJoin
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ckb on 2015/8/29.
 */
object PridectHistory {
  def main(args: Array[String]) {


    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val lda = LocalLDAModel.load(sc, "hdfs://10.3.12.9:9000/model/two")
    /*
    val input = lda.describeTopics(10).map(x => x._2).map(y => y.toVector)
    val index1 = input.zipWithIndex.map(x => {
        val lo = x._2.toLong
        (x._1, lo)
    })

    //println("get topic")
    val rddinput = sc.parallelize(index1, 10)
    val inputDev = input.toVector
    */
    // rdd.map(_.replaceAll("\\]|\\)", "").split("\\[")(1).split(",").map(_.toDouble)).map(_.toVector).collect
    //val vec=(x.replaceAll(s"\\]|\\)","").split("\\[")(1).split(",").map(_.toDouble)).map()
    /*
    val artRdd:RDD[String]=sc.textFile("hdfs://10.3.12.9:9000/test/PrWord/Word6/",10)


    val inputRdd=artRdd.map(x=>{x.replaceAll(s"\\]|\\)","").split("\\[")
      // x(0).replaceAll(s"\\(","").toLong
    }).map(y=>{
      val vec=(y(1).split(",").map(_.toDouble)).toVector
      val index=y(0).replaceAll(s"\\(|\\,","").toLong
      (vec,index)
    })
    */
    //val list = List("hdfs://10.3.12.9:9000/test/bbc/dataone")
    val list = List("hdfs://10.3.12.9:9000/test/VectorWord/HistoryVector/HistoryWord1")
    val docVec = ToVector.wordToVector1(sc, list, 100000, "hdfs://10.3.12.9:9000/test/stopword.dic")
    //docVec.saveAsTextFile("hdfs://10.3.12.9:9000/test/PrWord/Word9")
    val preDeV = lda.topicDistributions(docVec)
    preDeV.repartition(1).saveAsTextFile("hdfs://10.3.12.9:9000/test/PrWord/Word2")
      /*
      .map(x => x._2.toArray)
      .map(y => {
      y.toVector
    }).take(1)(0)

    println("get one doc topic")
    //val vec2 = knnJoin.knnJoin(inputRdd,preDeV, 5, 20, sc)
    vec2.repartition(1).saveAsTextFile("hdfs://10.3.12.9:9000/test/preDoc3")
    */
    println("success")

  }



}
