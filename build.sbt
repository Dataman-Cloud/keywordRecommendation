import AssemblyKeys._
assemblySettings

name := "omega-service"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sprayVersion = "1.3.2"
  val akkaVersion = "2.3.12"
  val slickVersion = "2.1.0"
  Seq(
    "io.spray" % "spray-routing_2.10" % sprayVersion,
    "io.spray" % "spray-can_2.10" % sprayVersion,
    "io.spray" % "spray-client_2.10" % sprayVersion,
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "com.typesafe.slick" %% "slick" % slickVersion,
    "io.spray" %% "spray-json"    % "1.3.2",
    "org.postgresql" % "postgresql" % "9.4-1201-jdbc41",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.5.1" % "provided",
    "org.apache.spark" % "spark-sql_2.10" % "1.5.1" % "provided",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1",
    "mysql" % "mysql-connector-java" % "5.1.35",
    "com.github.scopt" % "scopt_2.10" % "3.2.0",
    "org.jsoup" % "jsoup" % "1.8.2"
  )
}

resolvers ++= Seq(
  "Spray repository" at "http://repo.spray.io",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "The New Motion Public Repo" at "http://nexus.thenewmotion.com/content/groups/public/",
  "rediscala" at "https://raw.github.com/etaty/rediscala-mvn/master/releases/"  
)

