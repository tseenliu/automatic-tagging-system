package com.cathay.ddt.ats

import java.io.File

import scala.io.Source
import scala.collection.mutable.ListBuffer
import com.cathay.ddt.kafka.{FrontierMessage, TagMessageTest}
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Tse-En on 2017/12/20.
  */
object SqlParser extends EnvLoader {

  // test
//  val a = TagMessage("frontier-adw", "D", "event_cc_txn_m", yyyymm = Some("201801"), finish_time = System.currentTimeMillis())
//  val b = TagMessage("frontier-adw", "M", "event_bp_point_m", yyyymm = Some("201712"), finish_time = System.currentTimeMillis())
//  val c = TagMessage("frontier-adw", "D", "rd_a", yyyymmdd = Some("20180102"), finish_time = System.currentTimeMillis())
//  val d = TagMessage("frontier-adw", "D", "rd_b", yyyymmdd = Some("20180101"), finish_time = System.currentTimeMillis())
//  val e = TagMessage("frontier-adw", "D", "rd_c", yyyymmdd = Some("20180103"), finish_time = System.currentTimeMillis())
//  def getTagMessages(doc: TagDictionary): Iterator[TagMessage] = {
//    Iterator(a,b,c,d,e)
//  }

  val mappingFilePath = "/Users/Tse-En/Desktop/frontier_mapping"
  var sqlMList = new ListBuffer[(String, Message)]()
  var kafkaMList = new ListBuffer[(String, String)]()
  var sqlMTable: Map[String, Message] = Map()
  var kafaMTable: Map[String, String] = Map()

  // Convert to tagMessages using a mapping table
  def CovertTagMessage(doc: TagDictionary): Unit = { }

  // Convert frontier messages to tagMessages
  def CovertTagMessage(input: FrontierMessage, output: TagMessage): Unit = {}


  def getTagMessages(sql: String): Iterator[Message] = {
    val map = getSqlMTable
    Iterator(
      map("event_cc_txn"),
      map("event_bp_point"),
      map("rd_a"))
//    Iterator(a.getDefaultTM,b.getDefaultTM,c.getDefaultTM,d.getDefaultTM,e.getDefaultTM)
  }

  // Convert frontier messages to tagMessages
  def CovertTagMessage(topic: String, message: TagMessageTest): TagMessage = {
    val map = getkafkaMTable
    val frequency = map(message.value)
    TagMessage(
      topic,
      frequency,
      message.value,
      message.yyyymm,
      message.yyyymmdd,
      message.finish_time)
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
    if (kafaMTable.isEmpty) {
      initialFromLocal()
      kafaMTable
    } else {
      kafaMTable
    }
  }

  def initialFromLocal(): Unit = {
    sqlMTable = Map()
    kafaMTable = Map()
    for (line <- Source.fromFile(mappingFilePath).getLines) {
      val record = line.trim.split(",")
      val key = record(0)
      val value = record(1)
      val tmp = value.split(" ")
      kafkaMList += ((tmp(0), tmp(1)))
      sqlMList += ((key, SimpleTagMessage(tmp(1), tmp(0))))
    }
    sqlMTable = sqlMList.toMap
    kafaMTable = kafkaMList.toMap
  }

  def print(): Unit = {
    val m = getSqlMTable
    m.foreach(println(_))
  }



}
