package com.cathay.ddt.tagging.core

import akka.actor.Actor
import com.cathay.ddt.ats.TagScheduler.ScheduleInstance
import com.cathay.ddt.ats.TagState.{Daily, Monthly, Report}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import sys.process._

/**
  * Created by Tse-En on 2018/1/29.
  */
class TaggingScript extends Actor {
  import TaggingScript._

  override def receive: Receive = {
    case msg: Run =>
      println(s"I received Work Message and My ActorRef: ${self}")
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val execute = Seq("/bin/bash", s"/Users/Tse-En/Documents/gitRepo/automatic-tagging-system/example/runApp.sh") !

      //ProcessLogger(stdout append _ + "\n", stderr append _ + "\n")

      //println(execute, stdout, stderr)
      msg.instance.dic.update_frequency.toUpperCase() match {
        case "M" => context.actorSelection(s"/user/tag-manager/${msg.instance.dic.actorID}") ! Report(Monthly, msg.instance.dic)
        case "D" => context.actorSelection(s"/user/tag-manager/${msg.instance.dic.actorID}") ! Report(Daily, msg.instance.dic)
      }
    case SLEEP =>
      println(s"${self}: deading...")
      context.stop(self)
  }
}

object TaggingScript {
  case class Run(instance: ScheduleInstance)
  case object SLEEP
}
