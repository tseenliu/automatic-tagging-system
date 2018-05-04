package com.cathay.ddt.ats

import java.text.SimpleDateFormat

import scala.reflect._
import akka.persistence._
import akka.persistence.fsm._
import akka.persistence.fsm.PersistentFSM.FSMState
import com.cathay.ddt.ats.TagManager.{Cmd, StopTag}
import com.cathay.ddt.ats.TagScheduler.{Schedule, ScheduleInstance}
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.kafka.MessageProducer
import com.cathay.ddt.tagging.schema._
import com.cathay.ddt.tagging.schema.TagMessage._
import com.cathay.ddt.utils.CalendarConverter
import org.slf4j.LoggerFactory
import reactivemongo.bson.BSONDocument

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


/**
  * Created by Tse-En on 2017/12/28.
  */

object TagState {

  // Tag FSM States
  sealed trait State extends FSMState

  case object Waiting extends State {
    override def identifier = "Waiting"
  }

  case object Receiving extends State {
    override def identifier = "Receiving"
  }

  case object Running extends State {
    override def identifier = "Running"
  }

  case object Verifying extends State {
    override def identifier = "Verifying"
  }


  // Tag Data
  sealed trait Data {
    val requiredMessages: Set[SimpleTagMessage]
    val daily: Map[TagMessage, Boolean]
    val monthly: Map[TagMessage, Boolean]
    def isNull: Boolean = daily.isEmpty & monthly.isEmpty
    def isReady: Boolean = {
      val d =
        if(daily.values.toSet.isEmpty) true
        else if(daily.values.toSet.size == 1) daily.values.toSet.head
        else false

      val m =
        if(monthly.values.toSet.isEmpty) true
        else if(monthly.values.toSet.size == 1) monthly.values.toSet.head
        else false

      d && m
    }

    // check monthly is clear or not, when run successful
    def isMonthlyNull: Boolean = {
      if(monthly.nonEmpty) {
        val isFalse: Boolean = monthly.values.toSet.head
        if(monthly.values.toSet.size == 1 && !isFalse) true
        else false
      } else true
    }

  }

  case object ZeroMetadata extends Data {
    override val requiredMessages: Set[SimpleTagMessage] = Set()
    override val daily: Map[TagMessage, Boolean] = Map()
    override val monthly: Map[TagMessage, Boolean] = Map()
  }

  case class Metadata(override val requiredMessages: Set[SimpleTagMessage],
                      override val daily: Map[TagMessage, Boolean],
                      override val monthly: Map[TagMessage, Boolean]) extends Data {
    var monthlyAlreadyRun: Option[String] = None
    override def toString: String = {
      s"===================================================================" +
        s"\nRequired: $requiredMessages\nDaily: $daily\nMonthly[${monthlyAlreadyRun.getOrElse("None")}]: $monthly\n"
    }
  }

  case class TagMetadata(frequency: String, id: String, metadata: Metadata) {
    override def toString: String = {
      s"Tag($frequency, ID($id):\n$metadata"
    }
  }


  // Domain Events (Persist events)
  sealed trait DomainEvent

  case class RequiredMessages(requireTMs: Set[Message]) extends DomainEvent

  case class ReceivedMessage(tagMessage: TagMessage, `type`: FrequencyType) extends DomainEvent

  case class UpdatedMessages(`type`: FrequencyType) extends DomainEvent

  case class Reset(`type`: FrequencyType) extends DomainEvent

  case object ResetRanMonthly extends DomainEvent


  // Frequency Types
  sealed trait FrequencyType

  case object Daily extends FrequencyType

  case object Monthly extends FrequencyType



  // Commands
  case class Requirement(requireTMs: Set[Message])
  case class Receipt(tagMessage: TagMessage)
  case object Check
  case object Launch
  case class Report(startTime:Long, success: Boolean, frequencyType: FrequencyType, dic: ComposeTD)
  case object Stop
  case object GetStatus
  case object RevivalCheck
  case class Timeout(time: String)

}

class TagState(frequency: String, id: String) extends PersistentFSM[TagState.State, TagState.Data, TagState.DomainEvent] with CalendarConverter {
  import TagState._

  val logger = LoggerFactory.getLogger(this.getClass)

  override def preStart(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    context.system.scheduler.schedule(0 seconds, 1 seconds, self, Timeout(etlTime))
    logger.info(s"Tag($frequency), ID($id) is [UP].")
    self ! RevivalCheck
  }

  override def postStop(): Unit = {
    logger.info(s"Tag($frequency), ID($id) is [DOWN].")
  }

  def currentInst: TagMetadata = {
    TagMetadata(frequency, id, stateData.asInstanceOf[Metadata])
  }

  override def persistenceId: String = id

  override def applyEvent(evt: DomainEvent, currentData: Data): Data = {

    def resetDaily: Map[TagMessage, Boolean] = {
      val daily = collection.mutable.Map[TagMessage, Boolean]()
      currentData.daily.keys.foreach( k=> daily += (convertTM(convertSTM(k)) -> false) )
      daily.toMap
    }

    def resetMonthly: Map[TagMessage, Boolean] = {
      val monthly = collection.mutable.Map[TagMessage, Boolean]()
      currentData.monthly.keys.foreach( k => monthly += (convertTM(convertSTM(k)) -> false) )
      monthly.toMap
    }

    def isExist(simpleTagMessage: SimpleTagMessage, currentMap: Map[TagMessage, Boolean]): Boolean = {
      val tmp = currentMap.keySet.map(x => convertSTM(x))
      tmp(simpleTagMessage)
    }

    evt match {
      case RequiredMessages(requireTMs: Set[Message]) =>
        val messages = requireTMs.asInstanceOf[Set[SimpleTagMessage]]
        val daily = collection.mutable.Map[TagMessage, Boolean]()
        val monthly = collection.mutable.Map[TagMessage, Boolean]()
        messages.foreach { m =>
          m.update_frequency match {
            case "M" => monthly +=
              (if(isExist(m, currentData.monthly))
                currentData.monthly.keySet.filter(tm => isExist(convertSTM(tm), currentData.monthly)).head -> true
              else convertTM(m) -> false)
            case "D" => daily +=
              (if(isExist(m, currentData.daily))
                currentData.daily.keySet.filter(tm => isExist(convertSTM(tm), currentData.daily)).head -> true
              else convertTM(m) -> false)
//            case "M" => monthly += (m -> currentData.monthly.getOrElse(m, false))
//            case "D" => daily += (m -> currentData.daily.getOrElse(m, false))
            case _ => None
          }
        }
        Metadata(messages, daily.toMap, monthly.toMap)

      case ReceivedMessage(tm, Daily) =>
        if(tm.partition_values.get.contains(getDailyDate)) {
          // partition
          Metadata(currentData.requiredMessages, currentData.daily - convertTM(tm) + (tm -> true), currentData.monthly)
        }else if(tm.partition_fields.isEmpty && tm.partition_values.isEmpty){
          // 代碼
          Metadata(currentData.requiredMessages, currentData.daily - convertTM(tm) + (tm -> true), currentData.monthly)
        } else {
          currentData
        }

      case ReceivedMessage(tm, Monthly) =>
        if(tm.partition_values.get.contains(getLastMonth))
          Metadata(currentData.requiredMessages, currentData.daily, currentData.monthly - convertTM(tm) + (tm -> true))
        else currentData

      case UpdatedMessages(Daily) =>
        if (currentData.monthly.nonEmpty & currentData.daily.nonEmpty) {
          Metadata(currentData.requiredMessages, resetDaily, currentData.monthly)
        }else {
          Metadata(currentData.requiredMessages, resetDaily, resetMonthly)
        }

      case UpdatedMessages(Monthly) =>
        val updateCd = Metadata(currentData.requiredMessages, resetDaily, resetMonthly)
        updateCd.monthlyAlreadyRun = Some(getLastMonth)
        updateCd

      case Reset(frequencyType) =>
        frequencyType match {
          case Daily => Metadata(currentData.requiredMessages, resetDaily, currentData.monthly)
          case Monthly => Metadata(currentData.requiredMessages, resetDaily, resetMonthly)
        }

      case ResetRanMonthly =>
        currentData.asInstanceOf[Metadata].monthlyAlreadyRun = None
        currentData


    }
  }

  override def domainEventClassTag: ClassTag[DomainEvent] = classTag[DomainEvent]

  startWith(Waiting, ZeroMetadata)

  when(Waiting){
    case Event(Requirement(tmSet), _) =>
      goto(Receiving) applying RequiredMessages(tmSet) andThen { _ =>
        if (stateData.isNull) sender() ! false
        else sender() ! true
        saveStateSnapshot()
      }
  }

  when(Receiving){
    case Event(Requirement(tmSet), _) =>
      stay applying RequiredMessages(tmSet) andThen { _ =>
        if (stateData.isNull) sender() ! false
        else sender() ! true
        saveStateSnapshot()
        logger.info(s"Tag($frequency, ID($id):\n$stateData")
      }

    case Event(Receipt(tm), _) =>
      tm.update_frequency match {
        case "D" =>
          stay applying ReceivedMessage(tm, Daily) andThen { _ =>
            saveStateSnapshot()
            self ! Check
            logger.info(s"Tag($frequency, ID($id):\n$stateData")
          }
        case "M" =>
          stay applying ReceivedMessage(tm, Monthly) andThen{ _ =>
            saveStateSnapshot()
            self ! Check
            logger.info(s"Tag($frequency, ID($id):\n$stateData")
          }
        case _ =>
          logger.warn(s"Tag($frequency, ID($id): ${tm.update_frequency} match error.")
          stay()
      }

    case Event(Check, _) =>
      if(stateData.isReady){
        goto(Running) andThen{ _ =>
          self ! Launch
        }
      }else {
        stay()
      }

    case Event(Timeout(time), _) =>
      if (new SimpleDateFormat("HH:mm:ss").format(getCalendar.getTime).compareTo(time) == 0) {
        if(getCurrentDate.split("-")(2) == "01"){
          goto(Verifying) applying Reset(Monthly) andThen { _ =>
            self ! Check
          }
        } else {
          goto(Verifying) applying Reset(Daily) andThen { _ =>
            self ! Check
          }
        }

      } else stay()
  }

  when(Running) {
    case Event(Launch, _) =>
      val dic = Await.result(getDictionary, 10 second)
      if(dic.started.isDefined && dic.traced.isDefined) {
        frequency match {
          case "M" =>
            val composeTd = getComposedSql(Monthly, dic)
            context.actorSelection("/user/tag-scheduler") ! Schedule(ScheduleInstance(composeTd))
            stay()
          case "D" =>
            val composeTd = getComposedSql(Daily, dic)
            context.actorSelection("/user/tag-scheduler") ! Schedule(ScheduleInstance(composeTd))
            stay()
        }
      }else {
        // without started and traced value
//        val composeTd = getComposedSql(Daily, dic)
//        context.actorSelection("/user/tag-scheduler") ! Schedule(ScheduleInstance(composeTd))
//        stay()
        frequency match {
          case "M" =>
            val composeTd = getComposedSql(Monthly, dic)
            context.actorSelection("/user/tag-scheduler") ! Schedule(ScheduleInstance(composeTd))
            stay()
          case "D" =>
            val composeTd = getComposedSql(Daily, dic)
            context.actorSelection("/user/tag-scheduler") ! Schedule(ScheduleInstance(composeTd))
            stay()
        }
      }

    case Event(Report(startTime, success, frequencyType, ctd), _) =>
      // if success, produce and update
      if(success) {
        MessageProducer.getProducer.sendToFinishTopic(startTime, frequencyType, ctd, (stateData.daily ++ stateData.monthly).keySet.map(x => convertTM2(x)).toList)
        updateAndCheck(ctd)
      }else {
        logger.warn(s"Tag($frequency) ID[${ctd.actorID}] is not finish and goto Receiving state.")
        ctd.update_frequency match {
          case "M" => goto (Receiving) applying Reset(Monthly)
          case "D" => goto (Receiving) applying Reset(Daily)
        }
      }

  }
  def updateAndCheck(ctd: Dictionary): PersistentFSM.State[TagState.State, Data, DomainEvent] = {
    ctd.asInstanceOf[ComposeTD].update_frequency match {
      case "M" =>
          goto(Verifying) applying UpdatedMessages(Monthly) andThen { _ =>
            self ! Check
          }
      case "D" =>
        goto(Verifying) applying UpdatedMessages(Daily) andThen { _ =>
          self ! Check
        }
    }
  }

  when(Verifying){
    case Event(Check, metadata) =>
      if(metadata.isMonthlyNull) {
        context.parent ! Cmd(StopTag(id))
        stop()
      }else goto(Receiving)

    case Event(RevivalCheck, _) =>
      if(stateData.asInstanceOf[Metadata].monthlyAlreadyRun.isEmpty) {
        goto(Receiving)

      } else if(stateData.asInstanceOf[Metadata].monthlyAlreadyRun.get == getLastMonth) {
        logger.info(s"Tag($frequency) ID[$id] is already run in Month[${stateData.asInstanceOf[Metadata].monthlyAlreadyRun}].")
        context.parent ! Cmd(StopTag(id))
        stop()

      } else if(getCurrentDate >= getDayOfMonth(-numsOfDelayMonth)) {
        goto(Receiving)
      } else {
        goto(Receiving)
      }

    //    case Event(Remove, _) =>
    //      clearPersistentData()
    //      stop()

  }

  def clearPersistentData(): Unit = {
    (1L to lastSequenceNr) foreach deleteMessages
    deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = lastSequenceNr))
  }

  def getDictionary: Future[TagDictionary] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val query = BSONDocument("tag_id" -> id)
    MongoConnector.getTDCollection.flatMap(x => MongoUtils.findOneDictionary(x, query))
  }

  def getComposedSql(frequencyType: FrequencyType, dic: TagDictionary): ComposeTD = {
    if (dic.sql.contains("$")) {
      val startDate = getStartDate(frequencyType, dic.started.get)
      val endDate = getEndDate(frequencyType, startDate, dic.traced.get)
      ComposeTD(
        dic.tag_id,
        dic.source_type,
        dic.source_item,
        dic.tag_type,
        dic.tag_name,
        dic.sql.replaceAll("\\$start_date", startDate).replaceAll("\\$end_date", endDate),
        dic.update_frequency,
        dic.started,
        dic.traced,
        dic.score_method,
        dic.attribute,
        Some(startDate),
        Some(endDate),
        getCurrentDate,
        dic.system_name
      )
    }else {
      ComposeTD(
        dic.tag_id,
        dic.source_type,
        dic.source_item,
        dic.tag_type,
        dic.tag_name,
        dic.sql,
        dic.update_frequency,
        dic.started,
        dic.traced,
        dic.score_method,
        dic.attribute,
        None,
        None,
        getCurrentDate,
        dic.system_name
      )
    }

  }

  whenUnhandled {
    case Event(SaveSnapshotSuccess(metadata), _) ⇒
//      println(stateName)
      stay()

    case Event(SaveSnapshotFailure(metadata, reason), _) ⇒
//      println(s"save snapshot failed and failure is $reason")
      stay()

    case Event(GetStatus, _) =>
      stay() replying currentInst

    case Event(RevivalCheck, _) =>
//      println(s"Tag $frequency, $id: This state $stateName don't need to Revive")
      stay()

    case Event(Requirement(tmSet), _) =>
      sender() ! true
      stay()

    case Event(Timeout(time), _) =>
      stay()
  }
}
