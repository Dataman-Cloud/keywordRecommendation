package com.dataman.nlp

import java.io.BufferedInputStream
import java.net.URL
import java.util.Properties
import java.util.zip.GZIPInputStream

import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jsoup.Jsoup

/**
 * Created by fchen on 15-9-9.
 *
 * usage: sudo bin/spark-submit --class com.dataman.nlp.Stage1st /tmp/nlp-assembly-1.0.jar corpus/slate_article.csv
 */
object Stage1st {
  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)


    val hdfs = "10.3.12.9:9000"
//    val input = s"hdfs://$hdfs/" + args(0)
//
    val baseURL = "http://10.3.12.2:8666/analyzer"
//    val r1 = sc.textFile(input).map(_.replaceAll("""\\n""", ""))
//      .repartition(10)
//      .mapPartitions(iter => {
//      val props = new Properties
//      props.setProperty("sighanCorporaDict", baseURL)
//      props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
//      props.setProperty("inputEncoding", "UTF-8")
//      props.setProperty("sighanPostProcessing", "true")
//      val segmenter = new CRFClassifier[CoreLabel](props)
//      val url = s"$baseURL/ctb.gz"
//      val is = new URL(url).openStream
//      val inputStream = new GZIPInputStream(new BufferedInputStream(is))
//      segmenter.loadClassifierNoExceptions(inputStream, props)
//      inputStream.close
//      is.close
//      iter.map( record => {
//        if (record.length > 0) {
//          segmenter.segmentString(record).toArray.mkString(" ")
//        } else ""
//      })
//    })
    val dbhost = "10.3.12.10"
    val db = "ldadb"
    val user = "ldadev"
    val pw = "ldadev1234"
//    val table = "ibloomberg_content"
    val table = "ibloomberg_content"

    val r2 = sqlContext.read.format("jdbc").options(
      Map("url" -> s"jdbc:mysql://${dbhost}:3306/${db}?user=${user}&password=${pw}",
        "dbtable" -> table,
        "driver" -> "com.mysql.jdbc.Driver")).load()
    //val r2 = sqlContext.jdbc(s"jdbc:mysql://${dbhost}:3306/${db}?user=${user}&password=${pw}", table).select("content")
      .select("content", "keywords_char")
      .repartition(10)
      .mapPartitions(iter => {
      val props = new Properties
      props.setProperty("sighanCorporaDict", baseURL)
      props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
      props.setProperty("inputEncoding", "UTF-8")
      props.setProperty("sighanPostProcessing", "true")
      val segmenter = new CRFClassifier[CoreLabel](props)
      val url = s"$baseURL/ctb.gz"
      val is = new URL(url).openStream
      val inputStream = new GZIPInputStream(new BufferedInputStream(is))
      segmenter.loadClassifierNoExceptions(inputStream, props)
      inputStream.close
      is.close
      iter.map( record => {
        val doc = if (record(0).toString.length > 0 && record(0) != null) {
          val m = """\\r\\n|\\r|\\n|\\"""
          //val text = Jsoup.parse(record(0).toString.replaceAll(m, "")).text()
          val text = Jsoup.parse(record(0).toString.replaceAll(m, "")).text()
          //val text = record.toString.replaceAll(m, "")
          segmenter.segmentString(text).toArray.mkString(" ")
        } else ""
        doc + " " + record(1).toString
      })
    })

    clearData(r2).repartition(5).saveAsTextFile(s"hdfs://$hdfs/users/root/lda/documents/$table")

    def clearData(rdd: RDD[String]): RDD[String] = {
      rdd.map( line => {
        line.replaceAll( """\[ t \]|\[ \\/t \]""", "")
      }).map(_.split(" ")).map(list => {
        // 去掉包含英文字母，以及数值的单词
        list.filter(x => {
          """[1-9,0]""".r.findAllMatchIn(x).isEmpty && """[a-z,A-Z]""".r.findAllMatchIn(x).isEmpty
        }).mkString(" ")
      })
    }
  }
}
