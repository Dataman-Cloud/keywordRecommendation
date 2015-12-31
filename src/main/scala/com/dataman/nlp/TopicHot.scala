package com.dataman.nlp

import com.dataman.omega.service.utils.{Configs => C}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
 * Created by mymac on 15/9/18.
 */
object TopicHot {
  def getArticle(sc: SparkContext, sqlContext: SQLContext, appId: Int):String = {
    val ldaDocTopicPath = C.ldaDocTopicPath
    val artRdd = sc.textFile(ldaDocTopicPath)
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
        "url" -> s"jdbc:mysql://${C.mHost}:${C.mPort.toString}/${C.mDB}?user=${C.mUser}&password=${C.mPasswd}",
        "dbtable" -> "topics_10word",
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
