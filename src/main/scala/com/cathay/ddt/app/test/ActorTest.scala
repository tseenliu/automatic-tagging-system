package com.cathay.ddt.app.test

import akka.actor.ActorSystem
import com.cathay.ddt.ats.SegmentManager.{Cmd, ShowState}
import com.cathay.ddt.utils.EnvLoader


/**
  * Created by Tse-En on 2018/1/2.
  */
object ActorTest extends App with EnvLoader{

  val config = getConfig("api")
  val system = ActorSystem("remote", config)

//  val account = system.actorOf(Props(new Account("12345")), name = "alex")

//  val a = system.actorOf(Props[TimerActor], name = "alex")

//  Thread.sleep(1000)
//  system.terminate()

  val selection =
    system.actorSelection("akka.tcp://tag@127.0.0.1:2551/user/tag-manager")
  selection ! Cmd(ShowState)

}
