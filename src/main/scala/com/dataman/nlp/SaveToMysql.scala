package com.dataman.nlp

import java.io.BufferedInputStream
import java.util.Properties
import java.util.zip.GZIPInputStream
import java.net.{MalformedURLException, URL}
import com.dataman.nlp.util.{RelMysql,Mysql}
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.jsoup.Jsoup
// import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel,OnlineLDAOptimizer,EMLDAOptimizer,LDA,LocalLDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable
import java.text.BreakIterator
import java.sql.{ResultSet, DriverManager, PreparedStatement, Connection}
import org.apache.spark.rdd.{JdbcRDD,RDD}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
import com.dataman.nlp.util.RelMysql._
import com.dataman.nlp.util.StanfordSegment
/**
 * Created by ener on 8/24/15.
 */

object SaveToMysql {
/*
    def loadMysql(sc:SparkContext)={
      val sqlContext = new SQLContext(sc)
      val df=sqlContext.read.format("jdbc").options(
        Map(
          "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
          "dbtable" -> "article_20",
          "driver"->"com.mysql.jdbc.Driver"
        )).load().select("articleid","content").filter("content is not null")
      df
    }
*/
    def main(args: Array[String]) {
      val conf=new SparkConf()
      val sc =new SparkContext(conf)
      val sqlContext=new SQLContext(sc)
      sqlContext.read.format("jdbc").options(
        Map(
          "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
          "dbtable" -> "article_20",
          "driver"->"com.mysql.jdbc.Driver"
        )).load().repartition(10)
        .select("articleid","content").filter("content is not null")
        .mapPartitions(x=> {

       // val baseURL = "http://10.3.12.2:8666/analyzer"
       // val props = new Properties
       // props.setProperty("sighanCorporaDict", baseURL)
      //  props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
       // props.setProperty("inputEncoding", "UTF-8")
      //  props.setProperty("sighanPostProcessing", "true")
      //  val url = s"$baseURL/ctb.gz"
      //  val seg= new CRFClassifier[CoreLabel](props)
     //   val is = new java.net.URL(url).openStream()
     //   val inputStream = new GZIPInputStream(new BufferedInputStream(is))
    //    seg.loadClassifierNoExceptions(inputStream, props)
     //   inputStream.close
     //   is.close
        //val seg = StanfordSegment.wordSegment(baseURL)
        x.map(y=>{
          
          val baseURL = "http://10.3.12.2:8666/analyzer"
          val props = new Properties
          props.setProperty("sighanCorporaDict", baseURL)
          props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
          props.setProperty("inputEncoding", "UTF-8")
          props.setProperty("sighanPostProcessing", "true")
          val url = s"$baseURL/ctb.gz"
          val seg= new CRFClassifier[CoreLabel](props)
          val is = new java.net.URL(url).openStream()
          val inputStream = new GZIPInputStream(new BufferedInputStream(is))
          seg.loadClassifierNoExceptions(inputStream, props)
          inputStream.close
          is.close
          val artId=y(0).toString.toInt
          val text = Jsoup.parse(y(1).toString).text()
          val str=seg.segmentString(text).toArray.mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
          val docVec=ToVector.stringToRdd(sc,str)
          val localModel=LocalLDAModel.load(sc,"hdfs://10.3.12.9:9000/test/ModelMatrix/Matrix6")
          val docTopic =localModel.topicDistributions(docVec)
          val autoId=docTopic.map(_._1).toString().toInt
          val topicDes=docTopic.map(_._2.toArray.mkString(",")).toString()
          val conn_str = "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234&useUnicode=true&characterEncoding=utf8"
          val conn = DriverManager.getConnection(conn_str)
          val query = s"INSERT INTO ldadb.art_distri_on_topic (articleid , appid , theta) VALUES " + "(" + artId + ",'" + autoId + "','" + topicDes + "')"
          val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
          // do database insert
          try {
            val prep = conn.prepareStatement(query)
            //      val prep1 = conn.prepareStatement("update history set PV=900, UV = 110 where PV = 15")
            //    prep.setString(1, "Nothing great was ever achieved without enthusiasm.")
            //    prep.setString(2, "Ralph Waldo Emerson")
            prep.executeUpdate()
          }
          finally {
            conn.close
          }
          (artId,autoId,topicDes)
        })
      })



  }
}
