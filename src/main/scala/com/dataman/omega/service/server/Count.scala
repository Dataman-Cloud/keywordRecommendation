package com.dataman.omega.service.server

import java.io.BufferedInputStream
import java.net.URL
import java.util.Properties
import java.util.zip.GZIPInputStream

import com.dataman.omega.service.data.WordCountMsg
import com.dataman.webservice.Analyzer
import com.dataman.omega.service.utils.{Configs => C}
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jsoup.Jsoup

/**
 * Created by fchen on 15-10-20.
 */
object Count {
//  val sc = new SparkContext()
  def count(msg: WordCountMsg): String = {
    count(Analyzer.sc, msg.table)
  }
  def count(sc: SparkContext, table: String): String = {

    val sqlContext = new SQLContext(sc)
    val STOPWORD_PATH = C.ldaStopwordPath

    val ANALYZER_URL = C.analyzerURL
    val DB_HOST = C.mHost
    val DB_PORT = C.mPort
    val DB = C.mDB
    val USER = C.mUser
    val PASSWORD = C.mPasswd
    val ROW1 = "content"
    val ROW2 = "keywords_char"
    val keyWordWeight = 10

    val rdd = sqlContext.read.format("jdbc").options(
      Map("url" -> s"jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB}?user=${USER}&password=${PASSWORD}",
        "dbtable" -> table,
        "driver" -> "com.mysql.jdbc.Driver")).load()
      .select(ROW1, ROW2)
      .repartition(10)
      .mapPartitions(iter => {
        val props = new Properties
        props.setProperty("sighanCorporaDict", ANALYZER_URL)
        props.setProperty("serDictionary", s"$ANALYZER_URL/dict-chris6.ser.gz")
        props.setProperty("inputEncoding", "UTF-8")
        props.setProperty("sighanPostProcessing", "true")
        val segmenter = new CRFClassifier[CoreLabel](props)
        val url = s"$ANALYZER_URL/ctb.gz"
        val is = new URL(url).openStream
        val inputStream = new GZIPInputStream(new BufferedInputStream(is))
        segmenter.loadClassifierNoExceptions(inputStream, props)
        inputStream.close
        is.close
        iter.map( record => {
          val doc = if (record(0) != null && record(0).toString.length > 0) {
            val m = """\\r\\n|\\r|\\n|\\"""
            //val text = Jsoup.parse(record(0).toString.replaceAll(m, "")).text()
            val text = Jsoup.parse(record(0).toString.replaceAll(m, "")).text()
            //val text = record.toString.replaceAll(m, "")
            segmenter.segmentString(text).toArray.mkString(" ")
          } else ""
        val record_1 = if(record(1) != null && record(1).toString.length > 0) record(1).toString else ""
        (doc + (" " + record_1) * keyWordWeight).trim.split(" ").distinct.mkString(" ")
      })
    })

    def clearData(rdd: RDD[String]): RDD[Array[String]] = {
      rdd.map( line => {
        line.replaceAll( """\[ t \]|\[ \\/t \]""", "")
      }).map(_.split(" ")).map(list => {
        // 去掉包含英文字母，以及数值的单词
        list.filter(x => {
          """[1-9,0]""".r.findAllMatchIn(x).isEmpty && """[a-z,A-Z]""".r.findAllMatchIn(x).isEmpty
        })
      })
    }

    def filter(rdd: RDD[Array[String]]): RDD[String] = {
      val stopwords: Set[String] = sc.textFile(STOPWORD_PATH).map(_.trim).filter(_.size > 0).distinct.collect.toSet
      val broadcastsw = sc.broadcast(stopwords)
      rdd.flatMap(x => x).filter(x => x.size > 1 && !broadcastsw.value.contains(x))
    }

    val blackList = IgnoreService.selectTerms()

    filter(clearData(rdd)).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect.filterNot{
      case (w, c) => blackList.contains(w)
    }.take(500).map(x => {
      x._1 + ":" + x._2
    }).mkString(",")

  }
}
