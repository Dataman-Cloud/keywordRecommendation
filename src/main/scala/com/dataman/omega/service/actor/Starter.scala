package com.dataman.omega.service.actor

import akka.actor._
import akka.io.IO
import akka.routing.RoundRobinRouter
import spray.can.Http

import com.dataman.omega.service.utils.{Configs => C}
import com.dataman.omega.service.server.ServerSupervisor

object Starter {
  case object Start
  case object Stop
  case object StartRabbitMQ
  case object StartPublisher
}

class Starter extends Actor {
  import Starter.{ Start, StartRabbitMQ, StartPublisher }
  implicit val system = context.system

  def receive: Receive = {
    case Start =>
      val mainHandler: ActorRef =
        context.actorOf(Props[ServerSupervisor].withRouter(RoundRobinRouter(nrOfInstances = 10)))
      IO(Http) ! Http.Bind(mainHandler, interface = C.interface, port = C.appPort)
  }
}
