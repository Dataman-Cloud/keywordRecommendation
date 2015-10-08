package com.dataman.omega.service.server

import com.dataman.nlp.TopicHot
import com.dataman.omega.service.data._
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
  case class Ignores(id: Int, term: String, appid: Int)
  class ignoresTable(tag: Tag) extends Table[Ignores](tag, "ignores") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def term = column[String]("term")
    def appid = column[Int]("appid")

    def * = (id, term, appid) <> (Ignores.tupled, Ignores.unapply)
  }
  def db = Database.forURL(
    url = "jdbc:mysql://10.3.12.10:3306/ldadb?user=ldadev&password=ldadev1234&useUnicode=true&characterEncoding=utf8",
    //url = "jdbc:mysql://localhost:3306/test?user=root&password=&useUnicode=true&characterEncoding=utf8",
    driver = "com.mysql.jdbc.Driver"
  )
  implicit val session = db.createSession()
  val ignoreRaws = TableQuery[ignoresTable]

  def insertIgnoresTable(appid: Int, terms: Array[String]) = {
    terms.foreach ( term => {
      val raw = Ignores(0, term, appid)
      ignoreRaws += raw
    }
    )
  }
  def deleteIgnoresTable(appid: Int): Int = {
    ignoreRaws.filter(_.appid === appid).delete
  }
  def selectTerms(appid: Int): List[String] = {
    IgnoreService.ignoreRaws.filter(_.appid === appid).list.map{x => x.term}
  }

  def service(msg: String) = {
    val jsonMsg = Base64Util.decodeBase64withUTF8(msg)
    implicit val inputJsonFormat = jsonFormat2(IgnoreList)
    implicit val outputJsonFormat = jsonFormat2(IgnoreAck)
    val msgbean = jsonMsg.parseJson.convertTo[IgnoreList]
    val blackList = selectTerms(msgbean.appid)
    deleteIgnoresTable(msgbean.appid)
    insertIgnoresTable(msgbean.appid, msgbean.terms.get.split(","))

    val oum = IgnoreAck(msgbean.appid, Option("1"))
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

