package com.dataman.omega.service.actor

import akka.actor.{Props, Actor}
/**
 * Created by fchen on 15-7-30.
 */
object PostgresActor {
  case object FetchAll
  case class AddMarathon(clusterId: String)
}

class PostgresActor extends Actor {
  import PostgresActor._
  def receive: Receive = {
    case FetchAll =>
      //sender ! ClusterDAO.listAllCluster
  }
}