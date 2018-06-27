package com.cathay.ddt.tagging.core

import akka.actor.Actor
import com.cathay.ddt.ats.TagScheduler._
import com.cathay.ddt.ats.TagState.Report
import com.cathay.ddt.utils.CalendarConverter
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
      //sender tagState to produce start message
      context.actorSelection(s"/user/tag-manager/${msg.instance.composeCd.actorID}") ! Report(Start, startTime, msg.instance.composeCd)
      val command = Seq("/bin/bash", s"$runPath", "--job-name", s"${msg.instance.composeCd.tag_id}", "-p", s"$TMP_FILE_PATH${msg.instance.composeCd.tag_id}_$getCurrentDate")

      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val execute = command ! ProcessLogger(stdout append _ + "\n", stderr append _ + "\n")

      val frequencyType = msg.instance.composeCd.update_frequency.toUpperCase()
      if(execute == 0) {
        log.info(s"TagID[${msg.instance.composeCd.tag_id}] exit code: $execute")
        context.parent ! CompleteInstance(Finish, startTime, msg.instance)
      }else {
        log.error(s"TagID[${msg.instance.composeCd.tag_id}] exit code: $execute\n$stderr")
        context.parent ! CompleteInstance(NonFinish, startTime, msg.instance)
      }
  }
}

object TaggingRunner {
  case class Run(instance: ScheduleInstance)
}
