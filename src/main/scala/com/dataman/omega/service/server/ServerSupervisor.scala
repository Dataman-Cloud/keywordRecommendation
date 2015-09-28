package com.dataman.omega.service.server

import akka.actor._
import spray.http.StatusCodes

class ServerSupervisor extends Actor
with ClusterService
{
  def actorRefFactory = context
  def receive = runRoute(
    pathPrefix("rest" / "artipredict") {
      clusterServiceRoutes
    }
  )


}
