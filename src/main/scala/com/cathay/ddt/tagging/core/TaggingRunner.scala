package com.cathay.ddt.tagging.core

import akka.actor.Actor
import com.cathay.ddt.ats.TagScheduler.{FinishInstance, ScheduleInstance}
import com.cathay.ddt.ats.TagState.{Daily, Monthly, Report}
import com.cathay.ddt.utils.EnvLoader

import sys.process._

/**
  * Created by Tse-En on 2018/1/29.
  */
class TaggingRunner extends Actor with EnvLoader{
  import TaggingRunner._

  val runPath: String = getConfig("ats").getString("ats.spark.job-path")

  override def receive: Receive = {
    case msg: Run =>
      println(s"I received Work Message and My ActorRef: ${self}")
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val execute = Seq("/bin/bash", runPath) !

      //ProcessLogger(stdout append _ + "\n", stderr append _ + "\n")
      //println(execute, stdout, stderr)

      if(execute == 0) {
        msg.instance.composeTd.update_frequency.toUpperCase() match {
          case "M" =>
            //          context.parent ! FinishInstance(Monthly, msg.instance)
            context.actorSelection(s"/user/tag-manager/${msg.instance.composeTd.actorID}") ! Report(success = true, Monthly, msg.instance.composeTd)
          case "D" =>
            //          context.parent ! FinishInstance(Daily, msg.instance)
            context.actorSelection(s"/user/tag-manager/${msg.instance.composeTd.actorID}") ! Report(success = true, Daily, msg.instance.composeTd)
        }
      }

    case SLEEP =>
      println(s"${self}: deading...")
      context.stop(self)
  }
}

object TaggingRunner {
  case class BashHandler(exitCode: Int, stdout: StringBuilder, stderr: StringBuilder)

  case class Run(instance: ScheduleInstance)
  case object SLEEP
}
