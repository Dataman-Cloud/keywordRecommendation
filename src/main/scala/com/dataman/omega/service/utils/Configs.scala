package com.dataman.omega.service.utils

import org.slf4j.{ Logger, LoggerFactory }

object Configs {

  val interface  = "0.0.0.0"
  val appPort    = 8989

  val log: Logger = LoggerFactory.getLogger(this.getClass)
}
