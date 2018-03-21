package com.cathay.ddt.ats

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.routing.{BroadcastGroup, RoundRobinGroup}
import com.cathay.ddt.ats.TagState.{FrequencyType, Report}
import com.cathay.ddt.tagging.core.TaggingRunner
import com.cathay.ddt.tagging.core.TaggingRunner.{Run, SLEEP}
import com.cathay.ddt.tagging.schema.TagDictionary
import com.cathay.ddt.utils.YarnMetricsChecker

import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer

/**
  * Created by Tse-En on 2018/1/17.
  */
class TagScheduler extends Actor {
  import TagScheduler._

  val ymChecker: YarnMetricsChecker = YarnMetricsChecker.getChecker
//  var totalNumIns: Int = _

  override def preStart(): Unit = {
    println(s"[Info] ${self}: TagScheduler is [Start].")
//    totalNumIns = ymChecker.getYarnMetrics.get.getTotalInstance
    self ! Create(ymChecker.getYarnMetrics.get.getTotalInstance)
  }

  override def postStop(): Unit = {
    println(s"[Info] ${self}: TagScheduler is [Stop].")
  }

  var instanceList = new ListBuffer[ScheduleInstance]()
  var HscPaths = new ListBuffer[String]()
  var cancellable: Option[Cancellable] = None
  var routerPool: Option[ActorRef] = None

  def createSparkTagJob(nums: Int): Unit = {
    for( a <- 1 until nums+1) {
      context.actorOf(Props[TaggingRunner], s"runner$a")
      HscPaths += s"/user/tag-scheduler/runner$a"
    }
  }

//  val HscPaths = List(
//    "/user/tag-scheduler/w1",
//    "/user/tag-scheduler/w2",
//    "/user/tag-scheduler/w3",
//    "/user/tag-scheduler/w4",
//    "/user/tag-scheduler/w5",
//    "/user/tag-scheduler/w6"
//  )

  def schedulerPoolRun(): Unit = {
    if(routerPool.isEmpty){
      routerPool = Option(context.actorOf(RoundRobinGroup(HscPaths.toList).props(), "RoundRobinGroup"))
    }
    println("[Info] TagScheduler is Submitting.")
    instanceList.foreach { ins =>
      routerPool.get ! Run(ins)
    }
    instanceList.clear()
  }

  override def receive: Receive = {
    case Create(availableInstance) =>
      // call yarn rest api, and get number
      println(s"[Info] TagScheduler is create $availableInstance workers.")
      createSparkTagJob(availableInstance)

    case Schedule(instance) =>
      import scala.concurrent.ExecutionContext.Implicits.global
      println(s"[Info] TagScheduler is received: Tag(${instance.dic.update_frequency}) ID[${instance.dic.actorID}]")
      instanceList += instance

      if(cancellable.isDefined) {
        cancellable.get.cancel()
        println("[Info] TagScheduler Countdown timer is [Re-Start].")
      } else {
        println("[Info] TagScheduler Countdown timer is [Start].")
      }
      cancellable = Option(context.system.scheduler.scheduleOnce(10 seconds, self, RunInstances))

    case RunInstances =>
      schedulerPoolRun()

//      if(instanceList.lengthCompare(totalNumIns) > 0) {
//        schedulerPoolRun()
//        instanceList.remove(0, totalNumIns+1)
//        totalNumIns = 0
//      }else {
//        schedulerPoolRun()
//        instanceList.clear()
//        totalNumIns -= instanceList.length
//      }

    case FinishInstance(frequencyType, instance) =>
//      instanceList -= instance
//      totalNumIns += 1
      context.actorSelection(s"/user/tag-manager/${instance.dic.actorID}") ! Report(success = true, frequencyType, instance.dic)
      if(instanceList.nonEmpty) {
        self ! RunInstances
      }

    case KILL =>
      println("killing...")
      for( a <- 1 until 7) {
        context.actorSelection(s"/user/tag-scheduler/w$a") ! SLEEP
      }
  }
}

object TagScheduler {
  case class ScheduleInstance(composeSql: String, dic: TagDictionary)
  case class Schedule(instance: ScheduleInstance)
  case object RunInstances
  case class FinishInstance(frequencyType: FrequencyType, instance: ScheduleInstance)
  case class Create(availableInstance: Int)
  case object KILL
}
