package com.cathay.ddt.tagging.core

import akka.actor.Actor
import com.cathay.ddt.ats.TagScheduler.{FinishInstance, NonFinishInstance, ScheduleInstance}
import com.cathay.ddt.ats.TagState.{Daily, Monthly, Report}
import com.cathay.ddt.utils.{CalendarConverter, EnvLoader, HdfsClient}
import org.slf4j.LoggerFactory

import sys.process._

/**
  * Created by Tse-En on 2018/1/29.
  */
class TaggingRunner extends Actor with CalendarConverter{
  import TaggingRunner._

  val log = LoggerFactory.getLogger(this.getClass)

  val atsConfig = getConfig("ats")
  val runPath: String = atsConfig.getString("ats.spark.job-path")
  val TMP_FILE_PATH = {
    val path = atsConfig.getString("ats.hdfs.output-hdfsDir")
    if(path.last == '/') path
    else path + '/'
  }

  override def receive: Receive = {
    case msg: Run =>
      val startTime = getCalendar.getTimeInMillis/1000
      log.info(s"ActorRef: ${self} received Message by TagScheduler.")
      val command = Seq("/bin/bash", s"$runPath", "--job-name", s"${msg.instance.composeTd.tag_id}", "-p", s"$TMP_FILE_PATH${msg.instance.composeTd.tag_id}_$getCurrentDate")

      val execute = command !

      val frequencyType = msg.instance.composeTd.update_frequency.toUpperCase()
      if(execute == 0) {
        log.info(s"TagID[${msg.instance.composeTd.tag_id}] exit code: $execute")
        frequencyType match {
          case "M" => context.parent ! FinishInstance(startTime, Monthly, msg.instance)
          case "D" => context.parent ! FinishInstance(startTime, Daily, msg.instance)
        }
      }else {
        log.error(s"TagID[${msg.instance.composeTd.tag_id}] exit code: $execute")
        frequencyType match {
          case "M" => context.parent ! NonFinishInstance(startTime, Monthly, msg.instance)
          case "D" => context.parent ! NonFinishInstance(startTime, Daily, msg.instance)
        }
      }
  }
}

object TaggingRunner {
  case class Run(instance: ScheduleInstance)
}
