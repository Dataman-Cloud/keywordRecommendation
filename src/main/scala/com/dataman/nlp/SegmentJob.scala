package com.dataman.nlp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import java.util.Properties
import java.util.zip.GZIPInputStream
import java.io.{BufferedInputStream, PrintStream, Serializable}
import java.net.URL

import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import org.jsoup.Jsoup

object SegmentJob {
  def main(args: Array[String]) = {

    val hdfs = "10.3.12.9:9000"

    val input = s"hdfs://$hdfs/" + args(0)
    val spark = new SparkContext
    val sqlContext = new SQLContext(spark)
    val baseURL = "http://10.3.12.2:8666/analyzer"
    sqlContext.read.json(input).select("content")
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
          if (record.length > 0 && record(0) != null) {
            val text = Jsoup.parse(record(0).toString).text()
            segmenter.segmentString(text).toArray.mkString(" ")
          } else ""
        })
      }).saveAsTextFile(s"hdfs://$hdfs/users/root/lda/tmp")
  }
}


