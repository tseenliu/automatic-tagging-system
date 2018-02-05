package com.cathay.ddt.utils

import com.cathay.ddt.kafka.{FrontierMessage, TagFinishMessage}
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import com.cathay.ddt.tagging.schema.{CustomerDictionary, TagMessage}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by Tse-En on 2017/12/20.
  */
object MessageConverter extends CalendarConverter with EnvLoader {
  val mappingFilePath = getConfig("hive").getString("hive.mapping-path")
  var sqlMList = new ListBuffer[(String, Message)]()
  var kafkaMList = new ListBuffer[(String, String)]()
  var sqlMTable: Map[String, Message] = Map()
  var kafkaMTable: Map[String, String] = Map()

  def getRealDate(partitionValue: String): String = {
    partitionValue match {
      case x if x == getCurrentMonth => getDailyDate

      case x if x == getLastMonth =>
        if(getCurrentDate == getDayOfMonth(1)) getDailyDate
        else getLastDayOfMonth(partitionValue)

      case _ => getLastDayOfMonth(partitionValue)
    }
//    partitionValue == currentMon match {
//      case true =>
//        // Return currentDay - 2 day
//        getDailyDate
//      case false =>
//        if(partitionValue == getLastMonth) {
//          if(getCurrentDate == getDayOfMonth(1)) {
//            getDailyDate
//          } else getLastDayOfMonth(partitionValue)
//        }else {
//          getLastDayOfMonth(partitionValue)
//        }
//
//      //        getLastDayOfMonth(partitionValue)
//
//    }
  }

  // Convert to tagMessages using a mapping table
  def CovertTagMessage(doc: CustomerDictionary): Unit = { }


  // Convert frontier messages to tagMessages
  def CovertToTM(topic: String, input: FrontierMessage): TagMessage = {
    val value = s"${input.db}.${input.table}"
    val frequency = getkafkaMTable(value)
    frequency.toUpperCase() match {
      case "M" =>
        TagMessage(
          topic,
          frequency.toUpperCase(),
          value,
          Some(input.partition_values.head),
          None,
          input.exec_date)
      case "D" =>
        if(input.partition_fields.contains("yyyymm")
          && !input.partition_values.isEmpty) {
          TagMessage(
            topic,
            frequency.toUpperCase(),
            value,
            None,
            Some(getRealDate(input.partition_values.head)),
            input.exec_date)
        }else {
          TagMessage(
            topic,
            frequency.toUpperCase(),
            value,
            None,
            None,
            input.exec_date)
        }
    }
  }

  def CovertToTM(topic: String, input: TagFinishMessage): TagMessage = {
    input.update_frequency.toUpperCase() match {
      case "M" =>
        TagMessage(
          topic,
          input.update_frequency.toUpperCase(),
          input.tag_id,
          Some(input.yyyymm.getOrElse("")),
          None,
          input.finish_time)
      case "D" =>
        TagMessage(
          topic,
          input.update_frequency.toUpperCase(),
          input.tag_id,
          None,
          Some(input.yyyymmdd.getOrElse("")),
          input.finish_time)
    }

  }


  // parsing sql and get require value
  def getMessages(sql: String): Iterator[Message] = {
//    val tagPattern = s"""([tag_id\\s]+)\\=(["'a-z\\-\\_A-Z\\s]+)""".r
    val tagPattern = s"""([tag_id\\s]+)\\=(["'\\s]+)([\\-\\_a-zA-Z0-9]+)(["'\\s]+)""".r
    val matches = tagPattern.findAllIn(sql)
    val map = getSqlMTable
    var set = scala.collection.mutable.Set[Message]()
    matches.foreach{ x =>
      val tagPattern(a,b,c,d) = x
      set += map(c)
    }
    set.toIterator
  }

  def getSqlMTable: Map[String, Message] = {
    if (sqlMTable.isEmpty) {
      initialFromLocal()
      sqlMTable
    } else {
      sqlMTable
    }
  }

  def getkafkaMTable: Map[String, String] = {
    if (kafkaMTable.isEmpty) {
      initialFromLocal()
      kafkaMTable
    } else {
      kafkaMTable
    }
  }

  def initialFromLocal(): Unit = {
    sqlMTable = Map()
    kafkaMTable = Map()
    for (line <- Source.fromFile(mappingFilePath).getLines) {
      val record = line.trim.split(",")
      val key = record(0)
      val value = record(1)
      val tmp = value.split(" ")
      kafkaMList += ((tmp(0), tmp(1)))
      sqlMList += ((key, SimpleTagMessage(tmp(1), tmp(0))))
    }
    sqlMTable = sqlMList.toMap
    kafkaMTable = kafkaMList.toMap
  }

  def print(): Unit = {
    val m = getSqlMTable
    m.foreach(println(_))
  }



}
