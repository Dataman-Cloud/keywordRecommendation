package com.dataman.nlp

import com.dataman.omega.service.utils.{Configs => C}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.Properties
import java.util.zip.GZIPInputStream
import java.io.{BufferedInputStream, PrintStream, Serializable}
import java.net.{MalformedURLException, URL}
import org.apache.spark.rdd.JdbcRDD

import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import org.jsoup.Jsoup
import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.hadoop.hdfs.DistributedFileSystem
/**
 * Created by ener on 8/27/15.
 */


object SegmentJob_1 {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val baseURL= C.analyzerURL //the service shenkai provide
    //too slow
    /*
    val url="jdbc:mysql://10.3.12.10:3306/ldadb"
    val username = "ldadev"
    val password = "ldadev1234"
    Class.forName("com.mysql.jdbc.Driver").newInstance
    println("begin myrdd")
    val myRDD = new JdbcRDD( sc, () =>
      DriverManager.getConnection(url,username,password) ,
      "select content from article_20  limit ?,?",
      1,30000,10, r => r.getString("content"))
    */
    sqlContext.read.format("jdbc").options(
      Map(
        "url" -> s"jdbc:mysql://${C.mHost}:${C.mPort.toString}/${C.mDB}?user=${C.mUser}&password=${C.mPasswd}",
        "dbtable" -> s"${C.predictMysqlTableName}",
        "driver"->"com.mysql.jdbc.Driver"
      )).load()
      .repartition(10).select("articleid","content").filter("content is not null")
      .mapPartitions(iter => {
      println("begin stanford")
      val props = new Properties
      props.setProperty("sighanCorporaDict", baseURL)
      props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
      props.setProperty("inputEncoding", "UTF-8")
      props.setProperty("sighanPostProcessing", "true")
      val segmenter = new CRFClassifier[CoreLabel](props)
      val url = s"$baseURL/ctb.gz"
      val is = new java.net.URL(url).openStream()
      val inputStream = new GZIPInputStream(new BufferedInputStream(is))
      segmenter.loadClassifierNoExceptions(inputStream, props)
      inputStream.close
      is.close
      iter.map( record => {
        (record(0) ,
        if (record.length > 0 && record(1) != null)
        {
          val text = Jsoup.parse(record(1).toString).text()
          segmenter.segmentString(text).toArray.mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
        } else "" )
      }).map(y=>y._1+","+y._2)
    }).saveAsTextFile(C.ldaTrainDocsParticipated)
  }
}
