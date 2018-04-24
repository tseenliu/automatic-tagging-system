package com.cathay.ddt.app.test

import java.text.SimpleDateFormat
import java.util.Calendar

import akka.actor.Actor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by Tse-En on 2018/1/31.
  */
class TimerActor extends Actor {

//  def setTimer()(implicit ec: ExecutionContext): Future[Boolean] = Future {
//      var flag = true
//      while(flag) {
//        if (new SimpleDateFormat("HH:mm:ss.SSS").format(Calendar.getInstance().getTime).compareTo("10:14:00.000") == 0) {
//          Thread.sleep(1000)
//          flag = false
//        }
//      }
//    flag
//  }

  override def preStart(): Unit = {
    context.system.scheduler.schedule(0 seconds, 1 seconds, self, "yes")
    println(s"[Info] Tag is already serving...")

  }

  override def receive: Receive = {
    case "true" => println("12312312")
    case "yes" =>
      if (new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime).compareTo("11:02:00") == 0) {
        println("killll")
        context.stop(self)
      }
  }
}
