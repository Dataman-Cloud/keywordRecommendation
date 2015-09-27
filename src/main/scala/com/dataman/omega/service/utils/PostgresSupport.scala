package com.dataman.omega.service.utils

import slick.driver.PostgresDriver.simple._

trait PostgresSupport {
  def db = Database.forURL(
    url    = s"jdbc:postgresql://${Configs.pgHost}:${Configs.pgPort}/${Configs.pgDBName}?user=${Configs.pgUser}&password=${Configs.pgPassword}",
    driver = Configs.pgDriver
  )

  implicit val session: Session = db.createSession()
}
