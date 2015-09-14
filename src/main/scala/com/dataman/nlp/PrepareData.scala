package com.dataman.nlp

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext}
import org.jsoup.Jsoup

/**
 * Created by fchen on 15-9-8.
 */
class PrepareData {
  def main(args: Array[String]): Unit = {

    val dbhost = "10.3.12.11"

    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.jdbc("jdbc:mysql://10.3.12.11:3306/lda?user=root&password=1234567890", "article_20")

    import sqlContext.implicits._
    df.select("articleid", "content").map(row => {
      val id = row.getString(row.fieldIndex("articleid"))
      val html = row.getString(row.fieldIndex("content"))
      val doc = Jsoup.parse(html.replaceAll(" ", ""))
//      (id, doc.text)
      ("", "")
    }).toDF("id", "content").write.format("json").save("hdfs://lda/article")
  }
}
