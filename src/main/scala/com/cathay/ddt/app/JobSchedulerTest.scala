package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}
import com.cathay.ddt.ats.TagScheduler
import com.cathay.ddt.ats.TagScheduler.Run

/**
  * Created by Tse-En on 2018/1/29.
  */
object JobSchedulerTest extends App {

  val system = ActorSystem("Round-Robin-Router")
  val jobs = system.actorOf(Props[TagScheduler], "tag-scheduler")
  jobs ! Run

  Thread.sleep(10000)
}
