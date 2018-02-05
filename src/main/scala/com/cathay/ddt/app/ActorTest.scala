package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}

/**
  * Created by Tse-En on 2018/1/2.
  */
object ActorTest extends App {

  val system = ActorSystem("persistent-fsm-actors")

//  val account = system.actorOf(Props(new Account("12345")), name = "alex")

  val a = system.actorOf(Props[TimerActor], name = "alex")

//  Thread.sleep(1000)
//  system.terminate()

}
