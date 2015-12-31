package com.dataman.nlp.util

import com.dataman.omega.service.utils.{Configs => C}

import java.sql.{Connection, DriverManager, ResultSet, SQLException,PreparedStatement}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
/**
 * Created by ckb on 2015/8/12.
 */
object Mysql {
  def insert(query: String) {
    // Change to Your Database Config
    val conn_str = s"jdbc:mysql://${C.mHost}:${C.mPort.toString}/${C.mDB}?user=${C.mUser}&password=${C.mPasswd}&useUnicode=true&characterEncoding=utf8"
    // Load the driver
    //    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    //    try {
    // Configure to be Read Only
    //      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    // Execute Query
    //      val rs = statement.executeQuery("SELECT * FROM history LIMIT 5")
    // Iterate Over ResultSet
    //      while (rs.next) {
    //        println(rs.getString("PV"))
    //      }
    //    }
    //    finally {
    //      conn.close
    //    }
    // create database connection
    // val dbc = "jdbc:mysql://localhost:3306/DBNAME?user=DBUSER&password=DBPWD"
    // classOf[com.mysql.jdbc.Driver]
    // val conn = DriverManager.getConnection(dbc)
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
      if(conn != null)
      {conn.close}
    }
  }

  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog(name, count) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def saveToSQL(result: RDD[(String,Int)]): RDD[String] = {
    val table = "ldadb.topics"
    val value = result.map(x => {
      s"INSERT INTO $table (id, terms) VALUES " + "(" + x._2 + ",'" + x._1+ "')"
    })
    value
  }

  def sqlTo_topics_5word(result: RDD[(String,Int)]): RDD[String]={
    val table = C.predictTopic5WordTableName
    val value = result.map(x => {
      s"INSERT INTO $table (id, terms) VALUES " + "(" + x._2 + ",'" + x._1 + "')"
    })
    value

  }

  def saveToSQL10(result: RDD[(String,Int)]): RDD[String] = {
    val table = "ldadb.topics10"
    val value = result.map(x => {
      s"INSERT INTO $table (id, terms) VALUES " + "(" + x._2 + ",'" + x._1+ "')"
    })
    value
  }

  def sqlTo_topics_10word(result: RDD[(String,Int)]): RDD[String]={
    val table ="topics_10word"
    val value = result.map(x => {
      s"INSERT INTO $table (id, terms) VALUES " + "(" + x._2 + ",'" + x._1 + "')"
    })
    value
  }

  def sqlToArt_distri_on_topic(result: RDD[(Int,Int,String)]): RDD[String]={
    val table = "ldadb.art_distri_on_topic"
    val value = result.map(x => {
      s"INSERT INTO $table (articleid , appid , theta) VALUES " + "(" + x._2 + ",'" + x._1 + "','" + x._1 + "')"
    })
    value

  }

  /*
  def sqlToArt_distri_on_topic(sc:SparkContext,iterator: Iterator[(Int,Int,String)]): RDD[String]={
    val table = "ldadb.art_distri_on_topic"
    val value = iterator.map(x => {
      s"INSERT INTO $table (articleid , appid , theta) VALUES " + "(" + x._2 + ",'" + x._1 + "','" + x._1 + "')"
    })

    val result = sc.parallelize(value,10)

    result

  }
*/
  def batchProcessing(query: RDD[String]) {
    val conn_str = s"jdbc:mysql://${C.mHost}:${C.mPort.toString}/${C.mDB}?user=${C.mUser}&password=${C.mPasswd}&useUnicode=true&characterEncoding=utf8"
    query.foreachPartition(item => {
      val conn = DriverManager.getConnection(conn_str)
      val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
      try {
        conn.setAutoCommit(false)
        item.foreach(x => statement.addBatch(x))
        statement.executeBatch()
        println(" Update success! ")
        conn.commit()
        println(" Commit success! ")
        conn.setAutoCommit(true)
      } catch {
        case e: SQLException => {
          e.printStackTrace()
          if (!conn.isClosed()) {
            conn.rollback()
            println(" Rollback! ")
            conn.setAutoCommit(true)
          }
        }
      }
      finally {
        statement.close
        conn.close
      }
    })
  }

}
