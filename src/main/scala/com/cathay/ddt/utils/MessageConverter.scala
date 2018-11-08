package com.cathay.ddt.utils

import java.util.Calendar

import com.cathay.ddt.kafka.FinishMessage
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import com.cathay.ddt.tagging.schema.{SegmentDictionary, TagMessage}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
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
  def CovertTagMessage(doc: SegmentDictionary): Unit = { }


  // Convert finish messages to tagMessages
  def CovertToTM(topic: String, input: FinishMessage): TagMessage = {
    input.update_frequency.toUpperCase() match {
      case "M" =>
        val c = getCalendar
        c.setTime(SDF.parse(input.execute_time))
        c.add(Calendar.MONTH, numsOfDelayMonth)
        TagMessage(Some(topic), input.update_frequency.toUpperCase(), input.tag_id, Some("tag_id"), Some(getMonthFormat(c)), Some(input.finish_time))
      case "D" =>
        TagMessage(Some(topic), input.update_frequency.toUpperCase(), input.tag_id, Some("tag_id"), Some(input.execute_time), Some(input.finish_time))
    }
  }

  // parsing sql and get require value
  def getMessages(sql: String): Iterator[Message] = {
    val tagPattern = s"""(tag_id)(["'\\s\\=]+)([\\-\\_a-zA-Z0-9]+)(["']+)""".r
    val matches = tagPattern.findAllIn(sql)

    val map = getSqlMTable
    var set = scala.collection.mutable.Set[Message]()

    matches.foreach{ x =>
      val tagPattern(_,_,id,_) = x
      try {
        set += map(id)
      }catch {
        case keyNotFound: NoSuchElementException => log.error(s"tag id $id: $keyNotFound")
        case e: Throwable => log.error(s"${e.getMessage}")
      }
    }
    set.toIterator
  }

  def getSqlMTable: Map[String, Message] = {
    if (sqlMTable.isEmpty) {
      initialADW()
      sqlMTable
    } else sqlMTable

  }

  def initialADW(): Unit = {
    sqlMTable = Map()
    val f = ViewMapper.getViewMapper.initial()
    Await.result(f, 10 second)
    sqlMTable = ViewMapper.getViewMapper.getSqlMList.toMap
  }

}
