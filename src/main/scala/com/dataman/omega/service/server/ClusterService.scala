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

import com.dataman.omega.service.actor.PredictActor
import com.dataman.omega.service.actor.PredictActor._
import com.dataman.omega.service.server.HTTPHelpers._

trait ClusterService extends WebService {

  import com.dataman.omega.service.data.WordCountMsgJsonProtocol.wordCountMsgJsonFormat

  val work = actorRefFactory.actorOf(Props[PredictActor], "worker")

  def workCall(message: Any) =
    (work ? message).mapTo[String]

  val clusterServiceRoutes = {
    pathPrefix("pred") {
      post {
        formField('msg.as[String]) {
          msg => {
            complete(workCall(PredictArticleMsg(msg)))
          }
        }
      }
    } ~ pathPrefix("upload") {
      post {
        formField('msg.as[String]) {
          msg => {
            complete(UploadArticleService.service(msg))
          }
        }
      }
    } ~ pathPrefix("hot") {
      post {
        formField('msg.as[String]) {
          msg => {
            complete(TopicHotService.service(msg))
          }
        }
      }
    } ~ pathPrefix("ignore") {
      post {
        formField('msg.as[String]) {
          msg => {
            complete(IgnoreService.service(msg))
          }
        }
      }
    } ~ pathPrefix("wordcount") {
      post {
        entity(as[WordCountMsg]) { msg => {
          complete(Count.count(msg))
        }}
      }
    }
  }
}

