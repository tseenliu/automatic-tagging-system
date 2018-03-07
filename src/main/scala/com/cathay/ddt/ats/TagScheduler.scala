package com.cathay.ddt.ats

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.routing.{BroadcastGroup, RoundRobinGroup}
import com.cathay.ddt.tagging.core.TaggingScript
import com.cathay.ddt.tagging.core.TaggingScript.{Run, SLEEP}
import com.cathay.ddt.tagging.schema.TagDictionary
import scala.concurrent.duration._

import scala.collection.mutable.ListBuffer

/**
  * Created by Tse-En on 2018/1/17.
  */
class TagScheduler extends Actor {
  import TagScheduler._

  override def preStart(): Unit = {
    println(s"[Info] ${self}: TagScheduler is [Start].")
    self ! Create
  }

  override def postStop(): Unit = {
    println(s"[Info] ${self}: TagScheduler is [Stop].")
  }

//  var HscPaths = new ListBuffer[String]()
  var scheduleList = new ListBuffer[ScheduleInstance]()
  var cancellable: Option[Cancellable] = None

  //  context.actorOf(Props(new HippoReporter(reporterConfig, entry)), name = "reporter")

  def createSparkTagJob(nums: Int): Unit = {
    for( a <- 1 until nums) {
      context.actorOf(Props[TaggingScript], s"w$a")
//      HscPaths += s"/user/tag-scheduler/w$a"
    }
  }
  val HscPaths = List(
    "/user/tag-scheduler/w1",
    "/user/tag-scheduler/w2",
    "/user/tag-scheduler/w3",
    "/user/tag-scheduler/w4",
    "/user/tag-scheduler/w5",
    "/user/tag-scheduler/w6"
  )

  val routerPool: ActorRef =
    context.actorOf(RoundRobinGroup(HscPaths).props(), "RoundRobinGroup")

  override def receive: Receive = {
    case Create =>
      // call yarn rest api, and get number
      println(s"[Info] TagScheduler is create [7] workers.")
      createSparkTagJob(7)

    case Schedule(instance) =>
      import scala.concurrent.ExecutionContext.Implicits.global
      println(s"[Info] TagScheduler is received: Tag(${instance.dic.update_frequency}) ID[${instance.dic.actorID}]")
      scheduleList += instance

      if(cancellable.isDefined) {
        cancellable.get.cancel()
        println("[Info] TagScheduler Countdown timer is [Re-Start].")
        cancellable = Option(context.system.scheduler.scheduleOnce(10 seconds, self, RunInstances))
      } else {
        println("[Info] TagScheduler Countdown timer is [Start].")
        cancellable = Option(context.system.scheduler.scheduleOnce(10 seconds, self, RunInstances))
      }

    case RunInstances =>
      println("[Info] TagScheduler is Submitting.")
      scheduleList.foreach { ins =>
        routerPool ! Run(ins)
      }
      scheduleList.clear()

    case KILL =>
      println("killing...")
      for( a <- 1 until 7) {
        context.actorSelection(s"/user/tag-scheduler/w$a") ! SLEEP
      }
  }
}

object TagScheduler {
  val ACTIVE_NODES = 4
  val AVAIlABLE_MB = 217088
  val AVAIlABLE_VICTUAL_CORES = 42
  val AVAIlABLE_GB = 217088 / 1024
  val Threshold = 0.7

  case class ScheduleInstance(composeSql: String, dic: TagDictionary)
  case class Schedule(instance: ScheduleInstance)
  case object RunInstances
  case object Create
  case object KILL
}
