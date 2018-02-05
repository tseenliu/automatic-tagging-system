package com.cathay.ddt.ats

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.RoundRobinGroup
import com.cathay.ddt.tagging.core.SparkTagCalculate

/**
  * Created by Tse-En on 2018/1/17.
  */
class TagScheduler extends Actor {
  import TagScheduler._

  def createSparkTagJob() = {
    context.actorOf(Props[SparkTagCalculate], "w1")
  }

  val HscPaths = List(
    "/user/submitter/w1",
    "/user/submitter/w2",
    "/user/submitter/w3",
    "/user/submitter/w4",
    "/user/submitter/w5",
    "/user/submitter/w6"
  )

  val routerPool: ActorRef =
    context.actorOf(RoundRobinGroup(HscPaths).props(), "RoundRobinGroup")

  override def receive: Receive = {
    case Run =>
  }
}

object TagScheduler {
  val ACTIVE_NODES = 4
  val AVAIlABLE_MB = 217088
  val AVAIlABLE_VICTUAL_CORES = 42
  val AVAIlABLE_GB = 217088 / 1024
  val Threshold = 0.7
  case object Run
}
