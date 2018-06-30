package com.cathay.ddt.ats

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.routing.{BroadcastGroup, RoundRobinGroup}
import com.cathay.ddt.ats.TagState.{FrequencyType, Monthly, Report}
import com.cathay.ddt.tagging.core.TaggingRunner
import com.cathay.ddt.tagging.core.TaggingRunner.Run
import com.cathay.ddt.tagging.schema.ComposeCD
import com.cathay.ddt.utils.{CalendarConverter, HdfsClient, YarnMetricsChecker}

import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import com.cathay.ddt.tagging.protocal.ComposeCDProtocal._
import org.slf4j.LoggerFactory
import spray.json._

/**
  * Created by Tse-En on 2018/1/17.
  */
class TagScheduler extends Actor with CalendarConverter {
  import TagScheduler._

  val log = LoggerFactory.getLogger(this.getClass)
  val ymChecker: YarnMetricsChecker = YarnMetricsChecker.getChecker

  override def preStart(): Unit = {
    log.info(s"TagScheduler is [Start].")
    val yarnMetrics = YarnMetricsChecker.YARN_MASTERS.flatMap{ x =>
      try { YarnMetricsChecker.getChecker.getYarnMetrics(x) }
      catch { case foo: java.lang.IndexOutOfBoundsException => None }
    }.head
    self ! Create(yarnMetrics.getTotalInstance)
  }

  override def postStop(): Unit = {
    log.info(s"TagScheduler is [Stop].")
  }

  var instanceList = new ListBuffer[ScheduleInstance]()
  var HscPaths = new ListBuffer[String]()
  var cancellable: Option[Cancellable] = None
  var routerPool: Option[ActorRef] = None

  def createSparkTagJob(nums: Int): Unit = {
    for( a <- 1 until nums+1) {
      context.actorOf(Props[TaggingRunner], s"runner$a")
      HscPaths += s"/user/tag-manager/tag-scheduler/runner$a"
    }
  }

  def schedulerPoolRun(): Unit = {
    if(routerPool.isEmpty){
      routerPool = Option(context.actorOf(RoundRobinGroup(HscPaths.toList).props(), "RoundRobinGroup"))
    }
    log.info(s"TagScheduler is Submitting.")
    instanceList.foreach { ins =>
      routerPool.get ! Run(ins)
    }
    instanceList.clear()
  }

  override def receive: Receive = {
    case Create(availableInstance) =>
      // call yarn rest api, and get number
      log.info(s"TagScheduler is create $availableInstance workers.")
      createSparkTagJob(availableInstance)

    case Schedule(instance) =>
      import scala.concurrent.ExecutionContext.Implicits.global
      log.info(s"[Info] TagScheduler is received: Segment(${instance.composeCd.update_frequency}) ID[${instance.composeCd.actorID}]")

      val tdJson = instance.composeCd.toJson
      HdfsClient.getClient.write(fileName = s"${instance.composeCd.actorID}_$getCurrentDate", data = tdJson.compactPrint.getBytes)
      instanceList += instance

      if(cancellable.isDefined) {
        cancellable.get.cancel()
        log.info("[Info] TagScheduler Countdown timer is [Re-Start].")
      } else {
        log.info("[Info] TagScheduler Countdown timer is [Start].")
      }
      cancellable = Option(context.system.scheduler.scheduleOnce(20 seconds, self, RunInstances))

    case RunInstances =>
      schedulerPoolRun()

    case CompleteInstance(runStatus, startTime, instance) =>
      runStatus match {
        case Finish =>
          HdfsClient.getClient.delete(fileName = s"${instance.composeCd.actorID}_$getCurrentDate")
          context.actorSelection(s"/user/tag-manager/${instance.composeCd.actorID}") ! Report(Finish, startTime, instance.composeCd)
        case NonFinish =>
          context.actorSelection(s"/user/tag-manager/${instance.composeCd.actorID}") ! Report(NonFinish, startTime, instance.composeCd)
        case _ =>
      }
  }
}

object TagScheduler {
  sealed trait RunStatus
  case object Start extends RunStatus
  case object Finish extends RunStatus
  case object NonFinish extends RunStatus

  case class ScheduleInstance(composeCd: ComposeCD)
  case class Schedule(instance: ScheduleInstance)
  case object RunInstances
  case class CompleteInstance(runStatus: RunStatus, startTime:Long, instance: ScheduleInstance)
  case class Create(availableInstance: Int)
}
