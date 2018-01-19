package com.cathay.ddt.tagging

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Tse-En on 2017/12/11.
  */
object example extends App {

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

//  val df = spark.sql(
//    """
//      |select
//      |cutsomerID,
//      |cnt,
//      |item,
//      |txn_amt,
//      |txn_date
//      |from travel
//      |where txn_date between date_add(cast("2017-03-27" as date), -90) AND "2017-03-27"
//    """.stripMargin)
//  df.show

  val df = spark.sql(
    """
      |select
      |*
      |from travel
      |where txn_date between "2017-03-27" AND "2017-05-27"
    """.stripMargin)
  df.show
}
