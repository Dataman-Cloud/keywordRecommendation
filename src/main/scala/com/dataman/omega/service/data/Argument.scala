package com.dataman.omega.service.data

import spray.json.DefaultJsonProtocol

/**
 * Created by fchen on 15-9-24.
 */

case class Args(
  name: String,
  age: Int
)

object TestJsonProtocol extends DefaultJsonProtocol {
  implicit val testJsonFormat = jsonFormat2(Args)
}

