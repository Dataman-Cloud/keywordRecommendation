package com.dataman.omega.service

import akka.actor._
import com.dataman.omega.service.actor.Starter

import com.dataman.omega.service.utils.{ Configs => C }
import com.dataman.omega.service.utils.PostgresSupport

object Boot extends App
with PostgresSupport
{
  val system = ActorSystem("main-system")
  C.log.info(s"Actor system $system is up and running")

  val starter = system.actorOf(Props[Starter], name = "main")

  starter ! Starter.Start

}
