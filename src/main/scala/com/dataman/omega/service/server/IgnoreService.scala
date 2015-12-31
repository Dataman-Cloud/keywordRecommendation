package com.dataman.omega.service.server

import com.dataman.nlp.TopicHot
import com.dataman.omega.service.data._
import com.dataman.omega.service.utils.{Configs => C}
import com.dataman.webservice.{Analyzer, Base64Util}
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.slick.driver.JdbcProfile
import scala.slick.profile
import slick.driver.MySQLDriver.simple._



/**
 * Created by mymac on 15/9/29.
 */
object IgnoreService {

  val DB_HOST = C.mHost
  val DB = C.mDB
  val DB_USER = C.mUser
  val DB_PASSWORD = C.mPasswd
  val TABLE_IGNORES = "ignores"
  val DETAIL_SUCCESS = "1"
  val DETAIL_FAIL = "0"

  case class Ignores(id: Int, term: String, appid: Int)
  class ignoresTable(tag: Tag) extends Table[Ignores](tag, TABLE_IGNORES) {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def term = column[String]("term")
    def appid = column[Int]("appid")

    def * = (id, term, appid) <> (Ignores.tupled, Ignores.unapply)
  }


  def db = Database.forURL(
    url = s"jdbc:mysql://${DB_HOST}:3306/${DB}?user=${DB_USER}&password=${DB_PASSWORD}&useUnicode=true&characterEncoding=utf8",
    //url = "jdbc:mysql://localhost:3306/test?user=root&password=&useUnicode=true&characterEncoding=utf8",
    driver = "com.mysql.jdbc.Driver"
  )

  val ignoreRaws = TableQuery[ignoresTable]

  def insertIgnoresTable(appid: Int, terms: Array[String]) = {
    implicit val session = db.createSession()
    terms.foreach ( term => {
      val raw = Ignores(0, term, appid)
      ignoreRaws += raw
    })
    session.close()
  }
  def deleteIgnoresTable(appid: Int): Int = {
    implicit val session = db.createSession()
    val result = ignoreRaws.filter(_.appid === appid).delete
    session.close()
    result
  }
  def selectTerms(appid: Int): List[String] = {
    implicit val session = db.createSession()
    val result = IgnoreService.ignoreRaws.filter(_.appid === appid).list.map{x => x.term}
    session.close()
    result
  }
  def selectTerms(): List[String] = {
    implicit val session = db.createSession()
    val result = IgnoreService.ignoreRaws.list.map{x => x.term}
    session.close()
    result
  }

  def service(msg: String) = {
    val jsonMsg = Base64Util.decodeBase64withUTF8(msg)
    implicit val inputJsonFormat = jsonFormat2(IgnoreList)
    implicit val outputJsonFormat = jsonFormat2(IgnoreAck)
    val msgbean = jsonMsg.parseJson.convertTo[IgnoreList]
    deleteIgnoresTable(msgbean.appid)
    insertIgnoresTable(msgbean.appid, msgbean.terms.get.split(","))

    val oum = IgnoreAck(msgbean.appid, Option(DETAIL_SUCCESS))
    Base64Util.encodeUTF8String(oum.toJson.toString())
  }

 def main(args: Array[String]): Unit = {

  val jsonMsg =
    """
      |{
      | "appid": 13,
      | "terms": "今天,明天"
      |}
    """.stripMargin
    implicit val inputJsonFormat = jsonFormat2(IgnoreList)
    implicit val outputJsonFormat = jsonFormat2(IgnoreAck)
    val msgbean = jsonMsg.parseJson.convertTo[IgnoreList]
   val blackList = selectTerms(msgbean.appid)
   println("====")
   blackList.foreach(println)
deleteIgnoresTable(msgbean.appid)
    insertIgnoresTable(msgbean.appid, msgbean.terms.get.split(","))

    val oum = IgnoreAck(msgbean.appid, Option("0"))
  }
}
