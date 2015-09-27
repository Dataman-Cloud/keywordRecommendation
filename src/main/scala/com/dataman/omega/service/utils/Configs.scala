package com.dataman.omega.service.utils

import com.typesafe.config.ConfigFactory
import org.slf4j.{ Logger, LoggerFactory }

object Configs {
  val c = ConfigFactory.load()

  val interface  = c.getString("app.interface")
  val appPort    = c.getInt("app.port")
  val pgHost     = c.getString("postgres.host")
  val pgPort     = c.getInt("postgres.port")
  val pgDBName   = c.getString("postgres.dbname")
  val pgDriver   = c.getString("postgres.driver")

  val pgUser     = c.getString("postgres.user")
  val pgPassword = c.getString("postgres.password")

  val redisHost  = c.getString("redis.host")
  val redisPort  = c.getInt("redis.port")
  val tokenField = c.getString("redis.field")

  val inQueue    = c.getString("rabbitmq.in")
  val outQueue   = c.getString("rabbitmq.out")
  val rmqHost    = c.getString("rabbitmq.host")

  val log: Logger = LoggerFactory.getLogger(this.getClass)
}