package com.cathay.ddt.tagging

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Tse-En on 2017/12/13.
  */
object Sparkquery {

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

  def run(sql: String): DataFrame = {
    val df = spark.sql(sql)
    df
  }

//  val df = spark.sql("select * from travel")
//  df.show

}
