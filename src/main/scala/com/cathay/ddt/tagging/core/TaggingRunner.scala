package com.cathay.ddt.tagging.core

import akka.actor.Actor
import com.cathay.ddt.ats.TagScheduler.{FinishInstance, NonFinishInstance, ScheduleInstance}
import com.cathay.ddt.ats.TagState.{Daily, Monthly, Report}
import com.cathay.ddt.utils.{CalendarConverter, EnvLoader, HdfsClient}

import sys.process._

/**
  * Created by Tse-En on 2018/1/29.
  */
class TaggingRunner extends Actor with CalendarConverter{
  import TaggingRunner._

  val atsConfig = getConfig("ats")

  val runPath: String = atsConfig.getString("ats.spark.job-path")
  val TMP_FILE_PATH = {
    val path = atsConfig.getString("ats.hdfs.output-hdfsDir")
    if(path.last == '/') path
    else path + '/'
  }

  override def receive: Receive = {
    case msg: Run =>
      println(s"I received Run Message by TagScheduler and My ActorRef: ${self}")

      val command = Seq("/bin/bash", s"$runPath", "-p", s"$TMP_FILE_PATH + ${msg.instance.composeTd.tag_id}_${getCurrentDate}")
      println(s"script command: $command")
      val execute = command !
//        ProcessLogger(stdout append _ + "\n", stderr append _ + "\n")

      val frequencyType = msg.instance.composeTd.update_frequency.toUpperCase()
      if(execute == 0) {
        println(s"[INFO] exit code:$execute")
        frequencyType match {
          case "M" =>
            context.parent ! FinishInstance(Monthly, msg.instance)
//            context.actorSelection(s"/user/tag-manager/${msg.instance.composeTd.actorID}") ! Report(success = true, Monthly, msg.instance.composeTd)
          case "D" =>
            context.parent ! FinishInstance(Daily, msg.instance)
//            context.actorSelection(s"/user/tag-manager/${msg.instance.composeTd.actorID}") ! Report(success = true, Daily, msg.instance.composeTd)
        }
      }else {
        println(s"[ERROR] exit code:$execute")
        frequencyType match {
          case "M" =>  context.parent ! NonFinishInstance(Monthly, msg.instance)
            //context.actorSelection(s"/user/tag-manager/${msg.instance.composeTd.actorID}") ! Report(success = false, Monthly, msg.instance.composeTd)
          case "D" => context.parent ! NonFinishInstance(Daily, msg.instance)
            //context.actorSelection(s"/user/tag-manager/${msg.instance.composeTd.actorID}") ! Report(success = false, Daily, msg.instance.composeTd)
        }
      }

    case RunLocal =>
      println(s"I received Run Local Message by TagScheduler and My ActorRef: ${self}")

      val command = Seq("/bin/bash", s"$runPath", "-p", s"/Users/Tse-En/Desktop/pic")
      println(s"script command: $command")

      val execute = command !
//        ProcessLogger(stdout append _ + "\n", stderr append _ + "\n")

      if(execute == 0) {
        println(s"[INFO] exit code:$execute")
      }else {
        println(s"[INFO] exit code:$execute")
      }

    case SLEEP =>
      println(s"${self}: killing...")
//      context.stop(self)
  }
}

object TaggingRunner {

  case class Run(instance: ScheduleInstance)
  case object SLEEP

  case object RunLocal
}
