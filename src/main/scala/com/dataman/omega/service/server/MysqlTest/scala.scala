package com.dataman.omega.service.server.MysqlTest

import slick.driver.MySQLDriver.simple._
/**
 * Created by fchen on 15-9-25.
 */
object MysqlTest {
  def main(args: Array[String]): Unit = {
    case class Message(id: Int, title: String, content: String)
    class MessageTable(tag: Tag) extends Table[Message](tag, "message") {
      def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
      def title = column[String]("title")
      def content = column[String]("content")

      def * = (id, title, content) <> (Message.tupled, Message.unapply)
    }

    def db = Database.forURL(
      url = "jdbc:mysql://localhost:3306/test?user=root&password=",
      driver = "com.mysql.jdbc.Driver"
    )
    implicit val session = db.createSession()

    val messages = TableQuery[MessageTable]

    //createTable

    insertMessage

    def createTable() = {
      messages.ddl.create
    }

    def insertMessage = {
      val m1 = Message(0, "a", "b")
      messages += m1
      val m2 = Message(0, "b", "b")
      val m3 = Message(0, "c", "c")
      messages ++= m2 :: m3 :: Nil
    }

  }
}