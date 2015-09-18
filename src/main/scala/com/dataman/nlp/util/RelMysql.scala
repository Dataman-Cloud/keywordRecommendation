package com.dataman.nlp.util

/**
 * Created by ckb on 2015/8/20.
 */

import java.sql.{DriverManager,PreparedStatement,Connection,ResultSet}
import org.apache.spark.rdd.{JdbcRDD,RDD}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame,SQLContext}
object RelMysql {


  def getData(sc:SparkContext,tablename:String):JdbcRDD[String]={
    val url="jdbc:mysql://10.3.12.10:3306/ldadb"
    val user="ldadev"
    val password="ldadev1234"
    val reRdd= new JdbcRDD(sc,()=>{
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection(url,user,password)},
     "SELECT content from article_20 limit ?,? ",1,100,10,
      r=>r.getString("content")
    ).cache()
    reRdd
  }

  def insertTopics(iterator: Iterator[(String, Int)]): Unit ={
    val url="jdbc:mysql://10.3.12.10:3306/ldadb"
    val user="ldadev"
    val password="ldadev1234"
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into topics (terms,id,memo) values (?, ? ,?)"
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      conn = DriverManager.getConnection(url, user, password)
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.setString(3,null)
      })
    }
    finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
    }


   def dfData(sqlContext:SQLContext)={
     val df=sqlContext.read.format("jdbc").options(
       Map(
         "url" -> "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234",
         "dbtable" -> "article_20",
         "driver"->"com.mysql.jdbc.Driver"
       )).load().repartition(10).select("articleid","content").filter("content is not null").take(10)


       //sqlContext.read.jdbc("jdbc:mysql://10.3.12.10:3306","ldadb.article_20",proper).select("content")
       //sqlContext.read.json(input).select("content")


   }

   def main (args: Array[String]): Unit = {
     val conf=new SparkConf()
     val sc =new SparkContext(conf)
     val sqlContext=new SQLContext(sc)
     val df =dfData(sqlContext)
     print(df)

     /*
     val table ="article_20"
     val data = sc.parallelize(List(("www", 10), ("iteblog", 20), ("com", 30)),2)
     */
    // data.foreachPartition(insertData)
     //val testRdd=getData(sc,table).count()
  }



}
