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
    //    val tagPattern = s"""([tag_id\\s]+)\\=(["'\\s]+)([\\-\\_a-zA-Z0-9]+)(["'\\s]+)""".r
    //    val matches = tagPattern.findAllIn(sql)
    val map = getSqlMTable
    var set = scala.collection.mutable.Set[Message]()
//    matches.foreach{ x =>
//      val tagPattern(a,b,c,d) = x
//      set += map(c)
//    }
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
//      initialFromLocal()
      sqlMTable
    } else {
      sqlMTable
    }
  }

  def getKafkaMTable: Map[String, String] = {
    if (kafkaMTable.isEmpty) {
      initialADW()
//      initialFromLocal()
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
      val par = if(record(2) == "no") None else Some(record(2))
      val tmp = value.split(" ")
      kafkaMList += ((tmp(0), tmp(1)))
      sqlMList += ((key, SimpleTagMessage(tmp(1), tmp(0), par)))
    }
    sqlMTable = sqlMList.toMap
    kafkaMTable = kafkaMList.toMap
  }

  def initialADW(): Unit = {
    sqlMTable = Map()
    kafkaMTable = Map()
    ViewMapper.getViewMapper.initial()
    sqlMTable = ViewMapper.getViewMapper.getSqlMList.toMap
    kafkaMTable = ViewMapper.getViewMapper.getKafkaMList.toMap
  }

}
