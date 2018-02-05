package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}
import com.cathay.ddt.kafka.MessageConsumer
import com.cathay.ddt.utils.EnvLoader

/**
  * Created by Tse-En on 2017/12/26.
  */
object kafkaTest extends App with EnvLoader {

  val config = getConfig("kafka")
  val system = ActorSystem("alex")
  val kafkaActor = system.actorOf(Props(new MessageConsumer(config)), "kafka-test")
}
