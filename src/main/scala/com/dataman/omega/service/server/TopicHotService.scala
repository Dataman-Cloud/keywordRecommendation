package com.dataman.omega.service.server

import com.dataman.omega.service.data.{OutputMsg, InputMsg, ArticleID, DateSpan, HotTopic}
import com.dataman.webservice.{Analyzer, Base64Util}
import com.dataman.nlp.TopicHot
import spray.json.DefaultJsonProtocol
import spray.json._
import DefaultJsonProtocol._
import spray.routing.Route._
import spray.httpx.SprayJsonSupport._
import scala.collection.JavaConversions._

/**
 * Created by mymac on 15/9/28.
 */
object TopicHotService {
  def service(msg: String) = {
    val jsonMsg = Base64Util.decodeBase64withUTF8(msg)
    implicit val inputJsonFormat = jsonFormat3(DateSpan)
    implicit val outputJsonFormat = jsonFormat2(HotTopic)
    val msgbean = jsonMsg.parseJson.convertTo[DateSpan]
    val re = TopicHot.getArticle(Analyzer.sc,
                                 Analyzer.sqlContext,
                                 msgbean.appid)
    val oum = HotTopic(msgbean.appid, Some(re))
    Base64Util.encodeUTF8String(oum.toJson.toString())
  }
}

