package com.dataman.omega.service.server

import com.dataman.nlp.predictArticle
import com.dataman.omega.service.data.{Mysql, OutputMsg, InputMsg}
import com.dataman.webservice.{Analyzer, Base64Util}
import spray.json.DefaultJsonProtocol
import spray.json._
import DefaultJsonProtocol._
import spray.routing.Route._
import spray.httpx.SprayJsonSupport._
import scala.collection.JavaConversions._

/**
 * Created by mymac on 15/9/27.
 */
object PredictArticleService {
  def service(msg: String) = {
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

      Base64Util.encodeUTF8String(oum.toJson.toString())
  }
}

