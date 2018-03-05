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
    context.actorOf(RoundRobinGroup(HscPaths.toList).props(), "RoundRobinGroup")

  override def receive: Receive = {
    case Create =>
      println("creating...")
      // call yarn rest api, and get number
      createSparkTagJob(7)

    case Schedule(instance) =>
      import scala.concurrent.ExecutionContext.Implicits.global
      scheduleList += instance

      if(cancellable.isDefined) {
        println("killing...")
        cancellable.get.cancel()
        println("restarting...")
        cancellable = Option(context.system.scheduler.scheduleOnce(10 seconds, self, RunInstances))
      } else {
        println("starting...")
        cancellable = Option(context.system.scheduler.scheduleOnce(10 seconds, self, RunInstances))
      }

    case RunInstances =>
      println("running...")
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
