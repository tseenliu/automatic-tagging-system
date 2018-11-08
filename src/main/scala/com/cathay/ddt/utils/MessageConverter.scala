package com.cathay.ddt.utils

import com.cathay.ddt.kafka.{FrontierMessage, FinishMessage}
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
  * Created by Tse-En on 2017/12/20.
  */
object MessageConverter extends CalendarConverter {

  // local load
  val mappingFilePath = getConfig("ats").getString("ats.hive.local-path")

  val log = LoggerFactory.getLogger(this.getClass)

  var sqlMList = new ListBuffer[(String, Message)]()
  var kafkaMList = new ListBuffer[(String, String)]()
  var sqlMTable: Map[String, Message] = Map()
  var kafkaMTable: Map[String, String] = Map()

  def getRealDate(partitionValue: String): String = {
    val c = getCalendar
    c.setTime(SMF.parse(getDailyDate.split("-").dropRight(1).mkString("")))
    val pv = getCalendar
    pv.setTime(SMF.parse(partitionValue))

    if(pv.compareTo(c) == 0) {
      getDailyDate
    }else if(pv.before(c)) {
      getLastDayOfMonth(partitionValue)
    }else null
  }

  // Convert to tagMessages using a mapping table
  def CovertTagMessage(doc: TagDictionary): Unit = { }


  // Convert frontier messages to tagMessages
  def CovertToTM(topic: String, input: FrontierMessage): TagMessage = {
    val value = s"${input.db}.${input.table}"
    val frequency = getKafkaMTable(value)
    frequency.toUpperCase() match {
      case "M" =>
        TagMessage(Some(topic), frequency.toUpperCase(), value, Some(input.partition_fields.head), Some(input.partition_values.head), Some(input.exec_date))
      case "D" =>
        if(!input.partition_fields.contains("")
          && !input.partition_values.contains("")) {
          TagMessage(Some(topic), frequency.toUpperCase(), value, Some(input.partition_fields.head), Some(getRealDate(input.partition_values.head)), Some(input.exec_date))
        } else TagMessage(Some(topic), frequency.toUpperCase(), value, None, None, Some(input.exec_date))
    }
  }

  // parsing sql and get require value
  def getMessages(sql: String): Iterator[Message] = {
    val tablePattern = "(VP_BANK|vp_bank)\\.([a-z\\-\\_A-Z0-9]+)".r
    val matches = tablePattern.findAllIn(sql)
    val map = getSqlMTable
    var set = scala.collection.mutable.Set[Message]()
    matches.foreach{ x =>
      val table = x.trim.split("\\.")(1)
      try {
        set += map(table)
      }catch {
        case keyNotFound: NoSuchElementException => log.error(s"view table $table: $keyNotFound")
        case e: Throwable => log.error(s"${e.getMessage}")
      }
    }
    set.toIterator
  }

  def getSqlMTable: Map[String, Message] = {
    if (sqlMTable.isEmpty) {
      initialADW()
      sqlMTable
    } else {
      sqlMTable
    }
  }

  def getKafkaMTable: Map[String, String] = {
    if (kafkaMTable.isEmpty) {
      initialADW()
      kafkaMTable
    } else {
      kafkaMTable
    }
  }

  def initialADW(): Unit = {
    sqlMTable = Map()
    kafkaMTable = Map()
    ViewMapper.getViewMapper.initial()
    sqlMTable = ViewMapper.getViewMapper.getSqlMList.toMap
    kafkaMTable = ViewMapper.getViewMapper.getKafkaMList.toMap
  }

}
