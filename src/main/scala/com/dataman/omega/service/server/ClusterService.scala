package com.dataman.omega.service.server

import akka.actor._
import akka.io.IO
import akka.pattern.ask
import com.dataman.omega.service.data._
import com.dataman.webservice.{Analyzer, Base64Util}
import com.dataman.nlp.predictArticle
import spray.http.HttpHeaders.RawHeader
import spray.http.{HttpHeader, StatusCodes, HttpCharsets, MediaTypes}
import spray.json._
import DefaultJsonProtocol._
import spray.routing.Route._
import spray.httpx.SprayJsonSupport._
import scala.collection.JavaConversions._

import com.dataman.omega.service.actor.PostgresActor
import com.dataman.omega.service.server.HTTPHelpers._

trait ClusterService extends WebService {

  import PostgresActor._
  import com.dataman.omega.service.data.TestJsonProtocol.testJsonFormat

  val TOKEN = "authorization"

  val postgresWorker = actorRefFactory.actorOf(Props[PostgresActor], "postgres-worker")

  def postgresCall(message: Any) =
    (postgresWorker ? message).mapTo[String]

  val defaultResponseHeaders = RawHeader("Access-Control-Allow-Origin", "*") ::
    RawHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS") ::
    RawHeader("Access-Control-Allow-Headers", "Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control, X-XSRFToken, Authorization") :: Nil

  val clusterServiceRoutes = {
    pathPrefix("a") {
      post {
        entity(as[Args]) {
          args => {
            complete(args.name + "\t" + args.age)
          }
        }
      }
    } ~ pathPrefix("b") {
      post {
        formField('msg.as[String]) {
          msg => {
            complete(msg + "yma")
          }
        }
      }
    } ~ pathPrefix("predArt") {
      post {
        formField('msg.as[String]) {
          msg => {
            val jsonMsg = Base64Util.decodeBase64withUTF8(msg)
            implicit val inputJsonFormat = jsonFormat6(InputMsg)
            implicit val outputJsonFormat = jsonFormat6(OutputMsg)
            val msgbean = jsonMsg.parseJson.convertTo[InputMsg]
            Mysql.insertMessage(msgbean)
            val (articles, keywords) = predictArticle.getArticle(Analyzer.sc,
                                               Analyzer.sqlContext,
                                               Analyzer.model,
                                               msgbean.articleid,
                                               msgbean.appid,
                                               msgbean.content.get,
                                               Analyzer.analyzer)


            val oum = OutputMsg(msgbean.articleid, msgbean.appid, Some(keywords), Some(articles), Option(null), Option(null))
println(oum.toJson.toString())
            complete(oum.toJson.toString())
          }
        }
      }
    }
  }
}
