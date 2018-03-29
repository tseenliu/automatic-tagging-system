package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}
import com.cathay.ddt.tagging.core.TaggingRunner
import com.cathay.ddt.tagging.core.TaggingRunner.{RunLocal, SLEEP}

object RunnerTest extends App {

  val system = ActorSystem("persistent-fsm-actors")

  val runner = system.actorOf(Props[TaggingRunner], name = "runner")

  runner ! RunLocal

}
