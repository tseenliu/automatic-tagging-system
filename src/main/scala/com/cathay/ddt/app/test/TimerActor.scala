package com.cathay.ddt.app.test

import java.text.SimpleDateFormat
import java.util.Calendar

import akka.actor.Actor
import akka.actor.Timers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by Tse-En on 2018/1/31.
  */
class TimerActor extends Actor with Timers {

  override def preStart(): Unit = {
//    context.system.scheduler.schedule(0 seconds, 1 seconds, self, "yes")
    timers.startPeriodicTimer("key","yes",1.second)
    println(s"[Info] Tag is already serving...")

  }

  override def receive: Receive = {
    case "true" => println("haha")
    case "yes" =>
      println(new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime))
      if (new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime).compareTo("11:02:00") == 0) {
        println("killll")
        context.stop(self)
      }
  }
}
