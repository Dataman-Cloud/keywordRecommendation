import AssemblyKeys._
assemblySettings

name := "nlp"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
//  "net.sf.opencsv" % "opencsv" % "2.3",
  "com.github.scopt" % "scopt_2.10" % "3.2.0",
  "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.0" % "provided",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1",
  "org.jsoup" % "jsoup" % "1.8.2",
  "mysql" % "mysql-connector-java" % "5.1.36"
)
