package com.dataman.omega.service.utils

import com.typesafe.config.ConfigFactory
import org.slf4j.{ Logger, LoggerFactory }

private [dataman] object Configs {
  val c = ConfigFactory.load

  val interface  = c.getString("app.interface")
  val appPort    = c.getInt("app.port")

  val mHost = c.getString("mysql.host")
  val mPort = c.getInt("mysql.port")
  val mDB   = c.getString("mysql.db")
  val mUser = c.getString("mysql.user")
  val mPasswd = c.getString("mysql.password")

  private [dataman] val analyzerURL = c.getString("analyzer.baseURL")

  val ldaModelURI = c.getString("lda.model.uri")
  val ldaStopwordPath = c.getString("lda.stopword.path")
  val ldaVocabPath = c.getString("lda.vocab.path")
  val ldaDocTopicPath = c.getString("lda.doc.topic.path")
  val ldaTrainDocsParticipated = c.getString("lda.train.docs.participated")


  val ldaModelSavePath = c.getString("predict.model.uri")
  val trainTopicResultPath = c.getString("predict.topic.result.path")
  val trainVocabPath = c.getString("predict.vocab.path")

  val predictMysqlTableName = c.getString("predict.table")
  val predictTopic5WordTableName = c.getString("predict.topic5.word.table.name")
  val predictTopic10WordTableName = c.getString("predict.topic10.word.table.name")
}

