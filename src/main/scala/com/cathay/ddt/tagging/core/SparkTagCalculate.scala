package com.cathay.ddt.tagging.core

import akka.actor.Actor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Tse-En on 2018/1/29.
  */
class SparkTagCalculate extends Actor {

  //disable log showing of standard output
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL Example")
    .enableHiveSupport()
    .getOrCreate()

  override def receive: Receive = {
    case "run" =>
      val df = spark.sql(
        """
          |select
          |*
          |from travel
          |where txn_date = "2017-03-27"
        """.stripMargin)
      df.show
  }
}
