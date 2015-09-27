package com.dataman.nlp

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


object SegmentJob {
  def main(args: Array[String]) = {

    val input = "hdfs://10.3.12.9:9000/test/"
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val baseURL="http://10.3.12.2:8666/analyzer" //the service shenkai provide
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
        "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
      "dbtable" -> "train_data",
      "driver"->"com.mysql.jdbc.Driver"
    )).load().repartition(10).select("content").filter("content is not null")
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
          if (record.length > 0 && record(0) != null) {
          val text = Jsoup.parse(record(0).toString).text()
          segmenter.segmentString(text).toArray.mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
        } else ""
      })
    }).saveAsTextFile(args(0))

  }
}


