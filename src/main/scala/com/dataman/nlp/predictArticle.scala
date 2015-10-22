package com.dataman.nlp

import com.dataman.nlp.knn.knnJoin
import com.dataman.nlp.util.{Mysql, StanfordSegment}
import com.dataman.nlp.knn.knnJoin
import com.dataman.omega.service.server.IgnoreService
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jsoup.Jsoup
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import scala.compat.Platform
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import slick.driver.MySQLDriver.simple._

/**
 * Created by ener on 8/31/15.
 */
object predictArticle {

  def getArticle(sc:SparkContext,
                 sqlContext:SQLContext,
                 lda:LocalLDAModel,
                 articleId:Int,
                 appId:Int,
                 content:String,
                 analyzer:CRFClassifier[CoreLabel]):(String, String)={

    val DOC_TOPIC_RDD_PATH = "hdfs://10.3.12.9:9000/test/PrWord/Word2/"
    val STOPWORD_PATH = "hdfs://10.3.12.9:9000/test/stopword.dic"
    val DB_HOST = "10.3.12.10"
    val DB = "ldadb"
    val USER = "ldadev"
    val PASSWORD = "ldadev1234"
    val VOCAB_SIZE = 100000
    val TERM_NUM_PER_TOPIC = 10
    val TABLE_TOPIC_TERM = s"topics${TERM_NUM_PER_TOPIC.toString}"
    val RECOMMENDATION_DOC_NUM = 5

    val artRdd:RDD[String]=sc.textFile(DOC_TOPIC_RDD_PATH,10)
    val inputRdd=artRdd.map(x=>{x.replaceAll(s"\\]|\\)","").split("\\[")
    }).map(y=>{
      val vec=(y(1).split(",").map(_.toDouble)).toVector
      val index=y(0).replaceAll(s"\\(|\\,","").toLong
      (vec,index)
    })

    val text = Jsoup.parse(content).text()
    val result = analyzer.segmentString(text).toArray().mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
    println("result : " + result)
    val docVec = ToVector.strToVector(sc, result, VOCAB_SIZE, STOPWORD_PATH)

println("predict:")
println(Platform.currentTime)
    val preRdd = lda.topicDistributions(docVec)
println("end:")
println(Platform.currentTime)
    val preVec=preRdd.map(x => x._2.toArray).map(y => {y.toVector}).take(1)(0)
    val topidIndex=preRdd.map(x=>x._2.toArray).collect().take(1)(0)
    println("tipicIndex:")
    topidIndex.foreach(println)
    val value1=topidIndex.sorted.reverse.take(TERM_NUM_PER_TOPIC)
    val index  =new Array[Int](TERM_NUM_PER_TOPIC)
    for(a <- 0 to TERM_NUM_PER_TOPIC-1){
      index(a)=topidIndex.indexOf(value1(a))
    }
    val topic = sc.parallelize(index).zip(sc.parallelize(value1))
    val idStr=index.mkString(",")
    val sqlword=sqlContext.read.format("jdbc").options(
          Map(
            "url" -> s"jdbc:mysql://${DB_HOST}:3306/${DB}?user=${USER}&password=${PASSWORD}",
          "dbtable" -> TABLE_TOPIC_TERM,
          "driver"->"com.mysql.jdbc.Driver"
        )).load().repartition(10)
    sqlword.registerTempTable("word")
    val dfword =sqlContext.sql(s"select id,terms from word where id in ($idStr)").mapPartitions(x=>{
      x.map(y=> y(0).toString.toInt -> y(1).toString)
    })
    val word = dfword.join(topic).flatMap{case (index, (termVector, topicScore)) => {
      val array = termVector.replace("),("," ").replace(")","").replace("(","").split(" ")
      val termScore = array.map(y => {
        (y.substring(0, y.lastIndexOf(",")) -> (y.substring(y.lastIndexOf(",")+1, y.size ).toDouble*topicScore))
      })
      termScore
    }}.sortBy(_._2, ascending = false).take(200)//.mkString(",")
    // blacklist filter
    val blackList = IgnoreService.selectTerms(appId)
    val filterWord = word.filterNot{case (w, c) => blackList.contains(w)}.take(TERM_NUM_PER_TOPIC).mkString(",")

    println("get one doc topic")
println("begin knn:")
println(Platform.currentTime)
    val vec2 = knnJoin.knnJoin(inputRdd,preVec,RECOMMENDATION_DOC_NUM,sc)
    val artid=vec2.map(x=>x._2.toString).collect().mkString(",")
println("end knn:")
println(Platform.currentTime)
    sqlContext.clearCache()

    (artid, filterWord)
  }

  def main(args: Array[String])= {
    /*
    val arr =Array(1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 0.38348, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 0.19943, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 0.40035, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4, 1.7E-4)
    //val arr=Array(5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 0.34157, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 0.1858, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 0.37384, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 5.0E-5, 0.09349, 5.0E-5, 5.0E-5)
    val arr1 =arr.sorted.reverse.take(2)
    val arr2=new Array[Int](2)
    arr2(0)=arr.indexOf(arr1(0))
    arr2(1)=arr.indexOf(arr1(1))
    arr2.foreach(println)
    */
    val artid=4656
    val appid=4646
    val content="古代阿兹特克人发现了可可树，据说其国王每天要喝30杯，请看下面这张图，图片来自墨西哥，是两位印第安酋长在喝巧克力"
//    val list=getArticle(artid,appid,content)
//    println(list)

  }
}


