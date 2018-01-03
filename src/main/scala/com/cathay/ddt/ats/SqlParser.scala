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

  val filename = "/Users/Tse-En/Desktop/frontier_mapping"
  var mappingList = new ListBuffer[(String, SimpleTagMessage)]()
  var mapTable: Map[String, SimpleTagMessage] = Map()


  // test
  val a = TagMessage("frontier-adw", "D", "event_cc_txn_m", yyyymm = Some("201801"), finish_time = System.currentTimeMillis())
  val b = TagMessage("frontier-adw", "M", "event_bp_point_m", yyyymm = Some("201712"), finish_time = System.currentTimeMillis())
  val c = TagMessage("frontier-adw", "D", "rd_a", yyyymmdd = Some("20180102"), finish_time = System.currentTimeMillis())
  val d = TagMessage("frontier-adw", "D", "rd_b", yyyymmdd = Some("20180101"), finish_time = System.currentTimeMillis())
  val e = TagMessage("frontier-adw", "D", "rd_c", yyyymmdd = Some("20180103"), finish_time = System.currentTimeMillis())
  // test

  def getTagMessages(doc: TagDictionary): Iterator[TagMessage] = {
    Iterator(a,b,c,d,e)
  }

  def getTagMessages(sql: String): Iterator[Message] = {
    val mt = getMappingTable
    Iterator(
      mt("event_cc_txn"),
      mt("event_bp_point"),
      mt("rd_a"),
      mt("rd_b"),
      mt("rd_c"))
//    Iterator(a.getDefaultTM,b.getDefaultTM,c.getDefaultTM,d.getDefaultTM,e.getDefaultTM)
  }

  // Convert to tagMessages using a mapping table
  def CovertTagMessages(doc: TagDictionary): Unit = {

  }

  // Convert frontier messages to tagMessages
  def CovertTagMessages(input: FrontierMessage, output: TagMessage): Unit = {

  }

  // Convert frontier messages to tagMessages
  def CovertTagMessages(topic: String, message: TagMessageTest): TagMessage = {
    val mt = getMappingTable
    val result = mt(message.value)
    val tagMessage = TagMessage(
      topic,
      result.update_frequency,
      result.value,
      message.yyyymm,
      message.yyyymmdd,
      message.finish_time)
    tagMessage
  }

  def getMappingTable: Map[String, SimpleTagMessage] = {
    if (mapTable.isEmpty) {
      for (line <- Source.fromFile(filename).getLines) {
        val record = line.trim.split(",")
        val tmp = record(1).split(" ")
        mappingList += ((record(0), SimpleTagMessage(tmp(1), tmp(0))))
      }
      mapTable = mappingList.toMap
      mapTable
    } else {
      mapTable
    }
  }

  def print(): Unit = {
    val m = getMappingTable
    m.foreach(println(_))
  }



}
