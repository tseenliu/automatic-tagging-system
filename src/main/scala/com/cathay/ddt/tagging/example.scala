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
//    .master("local[*]")
    .appName("sparkshell-alex")
    .master("yarn-client")
//    .config("spark.driver.host","127.0.0.1")
    .config("spark.sql.warehouse","/user/hive/warehouse")
    .config("hive.metastore.uris","thrift://parhhdpm1:9083,thrift://parhhdpm2:9083")
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","30g")
    .config("spark.driver.cores","1")
    .config("spark.executor.cores","3")
    .config("spark.dynamicAllocation.enabled","true")
    .config("spark.shuffle.service.enabled","true")
    .enableHiveSupport()
    .getOrCreate()

//  val df = spark.sql(
//
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

//  val tmp =
//    s"""
//       |select
//       |*
//       |from travel
//       |where txn_date =      "2017-03-27
//     """.stripMargin.trim
//  println(tmp)

  println("starting.......")
  val df = spark.sql(
    """
      show databases
    """.stripMargin)
  df.show
}
