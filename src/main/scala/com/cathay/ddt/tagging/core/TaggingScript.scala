package com.cathay.ddt.tagging.core

import akka.actor.Actor
import com.cathay.ddt.ats.TagScheduler.ScheduleInstance
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
        ProcessLogger(stdout append _ + "\n", stderr append _ + "\n")

      println(execute, stdout, stderr)
//      context.actorSelection(s"../../${msg.instance.dic.actorID}")
    case SLEEP =>
      println(s"${self}: deading...")
      context.stop(self)
  }
}

object TaggingScript {
  case class Run(instance: ScheduleInstance)
  case object SLEEP
}
