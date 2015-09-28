package com.dataman.omega.service.data

import java.util.Date

import spray.json._
import DefaultJsonProtocol._

/**
 * Created by mymac on 15/9/25.
 */
case class InputMsg (
  articleid: Int,
  title: Option[String],
  subcontent: Option[String],
  content: Option[String],
  appid: Int,
  keywords: Option[String])

case class OutputMsg (
  articleid: Int,
  appid: Int,
  keywords: Option[String],
  related_arts: Option[String],
  topic_hot: Option[String],
  detail: Option[String]
)

case class DateSpan (
  startDate: Option[String],
  stopDate: Option[String],
  appid: Int
)

case class ArticleID (
  articleid: Int,
  appid: Int
)

case class HotTopic (
  appid: Int,
  topics: Option[String]
)

object Msgs extends App {
  implicit val aJsonFormat = jsonFormat6(InputMsg)
  val string =
    """
      |{"articleid":200013431,
      | "title":"驶向未来的梦想",
      | "subcontent":null,
      | "content":"二十五年来，中国社会经历着深刻而巨大的变革。",
      | "appid":20,
      | "keywords":null
      |}
    """.stripMargin
  val a = string.parseJson.convertTo[InputMsg]
  println(a.toJson.prettyPrint)
}
