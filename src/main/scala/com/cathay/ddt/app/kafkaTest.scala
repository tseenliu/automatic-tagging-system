package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}
import com.cathay.ddt.ats.EnvLoader
import com.cathay.ddt.kafka.MessageConsumer

/**
  * Created by Tse-En on 2017/12/26.
  */
object kafkaTest extends App with EnvLoader {

  val config = getConfig("tag")
  val system = ActorSystem("alex")
  val kafkaActor = system.actorOf(Props(new MessageConsumer(config)), "kafka-test")
}
