package com.dataman.nlp

import com.dataman.nlp.knn.knnJoin
import com.dataman.nlp.util.{Mysql, StanfordSegment}
import com.dataman.nlp.knn.knnJoin
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jsoup.Jsoup
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import scala.compat.Platform
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel

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
//    val sqlContext= new SQLContext(sc)
    val artRdd:RDD[String]=sc.textFile("hdfs://10.3.12.9:9000/test/PrWord/Word2/",10)
    val inputRdd=artRdd.map(x=>{x.replaceAll(s"\\]|\\)","").split("\\[")
    }).map(y=>{
      val vec=(y(1).split(",").map(_.toDouble)).toVector
      val index=y(0).replaceAll(s"\\(|\\,","").toLong
      (vec,index)
    })

    val text = Jsoup.parse(content).text()
    val baseURL="/var/analyzer"
    //val baseURL="http://10.3.12.2:8666/analyzer"
//    val result = StanfordSegment.articleSegment(baseURL).segmentString(text).toArray().mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
    val result = analyzer.segmentString(text).toArray().mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
    println("result : " + result)
    val docVec = ToVector.strToVector(sc, result, 100000, "hdfs://10.3.12.9:9000/test/stopword.dic")
//    println("docVec: ")
//    docVec.map(x => x._2.toArray).take(1)(0).foreach(println)
println("predict:")
println(Platform.currentTime)
    val preRdd = lda.topicDistributions(docVec)
println("end:")
println(Platform.currentTime)
    val preVec=preRdd.map(x => x._2.toArray).map(y => {y.toVector}).take(1)(0)
    println("preVec")
    preVec.toArray.foreach(println)
    val topidIndex=preRdd.map(x=>x._2.toArray).collect().take(1)(0)
      //.map(y=>y(0)).collect()
    println("tipicIndex:")
    topidIndex.foreach(println)
    val value1=topidIndex.sorted.reverse.take(10)
    val index  =new Array[Int](10)
    for(a <- 0 to 9){
      index(a)=topidIndex.indexOf(value1(a))
    }
    val topic = sc.parallelize(index).zip(sc.parallelize(value1))
    val idStr=index.mkString(",")
    println("idStr: " + idStr)
    val sqlword=sqlContext.read.format("jdbc").options(
          Map(
            "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
          "dbtable" -> "topics10",
          "driver"->"com.mysql.jdbc.Driver"
        )).load().repartition(10)
    sqlword.registerTempTable("word")
    val dfword =sqlContext.sql(s"select id,terms from word where id in ($idStr)").mapPartitions(x=>{
      x.map(y=> y(0).toString.toInt -> y(1).toString)
    })
println("dfword: " + dfword.count)
dfword.collect.foreach(println)
println("value1:")
value1.foreach(println)
    val word = dfword.join(topic).flatMap{case (index, (termVector, topicScore)) => {
      val array = termVector.replace("),("," ").replace(")","").replace("(","").split(" ")
      val termScore = array.map(y => {
        (y.substring(0, y.lastIndexOf(",")) -> (y.substring(y.lastIndexOf(",")+1, y.size ).toDouble*topicScore))
      })
      termScore
    }}.sortBy(_._2, ascending = false).take(10).mkString(",")
    println("word: " + word)
    println("get one doc topic")
println("begin knn:")
println(Platform.currentTime)
    val vec2 = knnJoin.knnJoin(inputRdd,preVec,5,10,sc)
    val artid=vec2.map(x=>x._2.toString).collect().mkString(",")
println("end knn:")
println(Platform.currentTime)
//    val retu=artid+":"+word
//    val retuLis = List(retu)
    sqlContext.clearCache()
//    retuLis

    (artid, word)
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
