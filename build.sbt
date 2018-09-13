
name := "automatic-tagging-system"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,

  "com.typesafe.akka" %% "akka-actor" % "2.5.16",
  "com.typesafe.akka" %% "akka-persistence" % "2.5.16",
  "com.typesafe.akka" %% "akka-remote" % "2.5.16",
  "com.typesafe.akka" %% "akka-http" % "10.0.9",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.9",

  "org.reactivemongo" %% "reactivemongo" % "0.12.1",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.iq80.leveldb" % "leveldb" % "0.7",
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
  "io.spray" %%  "spray-json" % "1.3.3",

  "net.cakesolutions" %% "scala-kafka-client" % "0.9.0.0" excludeAll(ExclusionRule(organization = "org.slf4j")),
  "net.cakesolutions" %% "scala-kafka-client-akka" % "0.9.0.0" excludeAll(ExclusionRule(organization = "org.slf4j")),

  "org.apache.hive" % "hive-jdbc" % "0.13.1",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0"
)