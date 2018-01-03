package com.cathay.ddt.app

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.cathay.ddt.ats.Account
import com.cathay.ddt.ats.Account._

/**
  * Created by Tse-En on 2018/1/2.
  */
object ActorTest extends App {

  val system = ActorSystem("persistent-fsm-actors")

  val account = system.actorOf(Props[Account])

  account ! Operation(1000, CR)

  account ! Operation(10, DR)

  Thread.sleep(1000)

  system.terminate()

}
