package com.dataman.nlp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
 * Created by mymac on 15/9/18.
 */
object TopicHot {
  def getArticle(sc: SparkContext, sqlContext: SQLContext, appId: Int):String = {
    val artRdd = sc.textFile("hdfs://10.3.12.9:9000/test/PrWord/Word1/")
    val inputRdd = artRdd.map(x=>{x.replaceAll(s"\\]|\\)","").split("\\[")
    }).map(y => {
      val vec = (y(1).split(",").map(_.toDouble)).toVector
      val index = y(0).replaceAll(s"\\(|\\,","").toLong
      (vec,index)
    })
    val hot = inputRdd.flatMap{ case (vec, index) => {
      val value = vec.toArray.sorted.reverse.filter(x => x > 0.4)
      val index = new Array[Int](value.length)
      for( a <- 0 to (value.length-1)) {
        index(a) = vec.indexOf(value(a))
      }
      index
    }}.map(x => (x, 1)).reduceByKey(_+_).sortBy(_._2, ascending = false)

    val sqlword = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
        "dbtable" -> "topics_5word",
        "driver"->"com.mysql.jdbc.Driver"
      )).load().repartition(10)
    sqlword.registerTempTable("word")

    val wordhot = hot.collect.map{ case (index, count) => {
      val dfword = sqlContext.sql(s"select id,terms from word where id in ($index)").mapPartitions(x => {
        x.map(y => y(1).toString)
      }).collect
      (dfword, count)
    }}

    sc.parallelize(wordhot).map(x => x._1.mkString(",") -> x._2).collect.mkString(",")
  }
}
