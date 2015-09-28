package com.dataman.omega.service.actor

import akka.actor.{Props, Actor}
import com.dataman.omega.service.server.PredictArticleService

object PredictActor {
  case class PredictArticleMsg(msg: String)
}

class PredictActor extends Actor {
  import PredictActor._
  def receive: Receive = {
    case PredictArticleMsg(msg) => {
      sender ! PredictArticleService.service(msg)
    }
  }
}
