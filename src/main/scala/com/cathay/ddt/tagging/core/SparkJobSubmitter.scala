package com.cathay.ddt.tagging.core

import akka.actor.Actor

/**
  * Created by Tse-En on 2018/1/29.
  */
class SparkJobSubmitter extends Actor {

  override def receive: Receive = {
    case "" => println(1)
  }
}
