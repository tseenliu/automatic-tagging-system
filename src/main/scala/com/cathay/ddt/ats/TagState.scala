package com.cathay.ddt.ats

import java.text.SimpleDateFormat
import scala.reflect._
import akka.actor.ActorRef
import akka.persistence._
import akka.persistence.fsm._
import akka.persistence.fsm.PersistentFSM.FSMState
import com.cathay.ddt.ats.TagManager.{Cmd, StopTag}
import com.cathay.ddt.ats.TagScheduler._
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.kafka.MessageProducer
import com.cathay.ddt.tagging.schema._
import com.cathay.ddt.tagging.schema.TagMessage._
import com.cathay.ddt.utils.CalendarConverter
import org.slf4j.{Logger, LoggerFactory}
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
    var rerty: Int = 0
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
  case class Requirement(frequency: String, requireTMs: Set[Message])
  case class Receipt(tagMessage: TagMessage)
  case object Check
  case object Launch
  case class Report(status:RunStatus, startTime:Long, dic: ComposeTD)
  case object Stop
  case object GetStatus
  case object RevivalCheck
  case object Revival
  case class Timeout(time: String)

}

class TagState(frequency: String, id: String, schedulerActor: ActorRef) extends PersistentFSM[TagState.State, TagState.Data, TagState.DomainEvent] with CalendarConverter {
  import TagState._

  var freq: String = frequency
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def preStart(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    context.system.scheduler.schedule(0 seconds, 1 seconds, self, Timeout(etlTime))
    logger.info(s"Tag($freq), ID($id) is [UP].")
    self ! RevivalCheck
  }

  override def postStop(): Unit = {
    logger.info(s"Tag($freq), ID($id) is [DOWN].")
  }

  def currentInst: TagMetadata = {
    TagMetadata(freq, id, stateData.asInstanceOf[Metadata])
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
              (if(isExist(m, currentData.monthly)) {
                val value = currentData.monthly.keySet.filter(tm => m == convertSTM(tm)).head
                value -> currentData.monthly.getOrElse(value, false)
              } else convertTM(m) -> false)
            case "D" => daily +=
              (if(isExist(m, currentData.daily)) {
                val value = currentData.daily.keySet.filter(tm => m == convertSTM(tm)).head
                value -> currentData.daily.getOrElse(value, false)
              } else convertTM(m) -> false)
            case _ => None
          }
        }
        currentData match {
          case ZeroMetadata => Metadata(messages, daily.toMap, monthly.toMap)
          case _ =>
            val newMd = Metadata(messages, daily.toMap, monthly.toMap)
            newMd.monthlyAlreadyRun = currentData.asInstanceOf[Metadata].monthlyAlreadyRun
            newMd
        }

      case ReceivedMessage(tm, Daily) =>
        if(tm.partition_fields.isDefined && tm.partition_values.get.contains(getDailyDate)) {
          // partition
          Metadata(currentData.requiredMessages, currentData.daily - convertTM(tm) + (tm -> true), currentData.monthly)
        }else if(tm.partition_fields.isEmpty && tm.partition_values.isEmpty){
          // 代碼
          Metadata(currentData.requiredMessages, currentData.daily - convertTM(tm) + (tm -> true), currentData.monthly)
        } else {
          currentData
        }

      case ReceivedMessage(tm, Monthly) =>
        if(tm.partition_fields.isDefined && tm.partition_values.get.contains(getLastMonth))
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
    case Event(Requirement(f, tmSet), _) =>
      freq = if(freq == f) freq else f
      goto(Receiving) applying RequiredMessages(tmSet) andThen { _ =>
        if (stateData.isNull) sender() ! false
        else sender() ! true
        saveStateSnapshot()
      }
  }

  when(Receiving){
    case Event(Requirement(f, tmSet), _) =>
      freq = if(freq == f) freq else f
      stay applying RequiredMessages(tmSet) andThen { _ =>
        if (stateData.isNull) sender() ! false
        else sender() ! true
        saveStateSnapshot()
        logger.info(s"Tag($freq, ID($id):\n$stateData")
      }

    case Event(Receipt(tm), _) =>
      tm.update_frequency match {
        case "D" =>
          stay applying ReceivedMessage(tm, Daily) andThen { _ =>
            saveStateSnapshot()
            self ! Check
            logger.info(s"Tag($freq, ID($id):\n$stateData")
          }
        case "M" =>
          stay applying ReceivedMessage(tm, Monthly) andThen{ _ =>
            saveStateSnapshot()
            self ! Check
            logger.info(s"Tag($freq, ID($id):\n$stateData")
          }
        case _ =>
          logger.warn(s"Tag($freq, ID($id): ${tm.update_frequency} match error.")
          stay()
      }

    case Event(Check, _) =>
      if(stateData.isReady){
        goto(Running) andThen{ _ =>
          saveStateSnapshot()
          self ! Launch
        }
      }else {
        stay()
      }

    case Event(Timeout(time), _) =>
      if (new SimpleDateFormat("HH:mm:ss").format(getCalendar.getTime).compareTo(time) == 0) {
        val resetDay =
          time.compareTo("00:00:00") match {
            case 0 =>
              numsOfDelayDate.abs.toString
            case _ =>
              if((numsOfDelayDate.abs-1).toString.length == 1)
                s"0${(numsOfDelayDate.abs-1).toString}"
              else (numsOfDelayDate.abs-1).toString
          }

        if(getCurrentDate.split("-")(2) == resetDay) {
          goto(Verifying) applying Reset(Monthly) andThen { _ =>
            saveStateSnapshot()
            self ! Check
          }
        } else {
          goto(Verifying) applying Reset(Daily) andThen { _ =>
            saveStateSnapshot()
            self ! Check
          }
        }

      } else stay()
  }

  when(Running) {
    case Event(Launch, _) =>
      val dic = Await.result(getDictionary, 10 second)
      if(dic.started.isDefined && dic.traced.isDefined) {
        freq match {
          case "M" =>
            val composeTd = getComposedSql(Monthly, dic)
            schedulerActor ! Schedule(ScheduleInstance(composeTd))
            stay()
          case "D" =>
            val composeTd = getComposedSql(Daily, dic)
            schedulerActor ! Schedule(ScheduleInstance(composeTd))
            stay()
        }
      }else {
        // tag dic's started and traced is not Defined
        freq match {
          case "M" =>
            val composeTd = getComposedSql(Monthly, dic)
            schedulerActor ! Schedule(ScheduleInstance(composeTd))
            stay()
          case "D" =>
            val composeTd = getComposedSql(Daily, dic)
            schedulerActor ! Schedule(ScheduleInstance(composeTd))
            stay()
        }
      }

    case Event(Report(status, startTime, ctd), _) =>
      // if success, produce and update
      status match {
        case Start =>
          MessageProducer.getProducer.sendToStart(startTime, ctd, stateData.requiredMessages.toList)
          stay()
        case Finish =>
          MessageProducer.getProducer.sendToFinishTopic(startTime, ctd, (stateData.daily ++ stateData.monthly).keySet.map(x => convertTM2(x)).toList, is_success = true)
          updateAndCheck(ctd)
        case NonFinish =>
          stateData.asInstanceOf[Metadata].rerty += 1
          if (stateData.asInstanceOf[Metadata].rerty==3) {
            MessageProducer.getProducer.sendToFinishTopic(startTime, ctd, (stateData.daily ++ stateData.monthly).keySet.map(x => convertTM2(x)).toList, is_success = false)
            logger.warn(s"Tag($freq) ID[${ctd.actorID}] is not finish and goto Receiving state.")
            ctd.update_frequency match {
              case "M" => goto (Receiving) applying Reset(Monthly)
              case "D" => goto (Receiving) applying Reset(Daily)
            }
          } else {
            goto(Running) andThen{ _ =>
              saveStateSnapshot()
              self ! Launch
            }
          }
      }
  }
  def updateAndCheck(ctd: Dictionary): PersistentFSM.State[TagState.State, Data, DomainEvent] = {
    ctd.asInstanceOf[ComposeTD].update_frequency match {
      case "M" =>
          goto(Verifying) applying UpdatedMessages(Monthly) andThen { _ =>
            saveStateSnapshot()
            self ! Check
          }
      case "D" =>
        goto(Verifying) applying UpdatedMessages(Daily) andThen { _ =>
          saveStateSnapshot()
          self ! Check
        }
    }
  }

  when(Verifying){
    case Event(Requirement(f, tmSet), _) =>
      freq = if(freq == f) freq else f
      stay() applying RequiredMessages(tmSet) andThen { _ =>
        if (stateData.isNull) sender() ! false
        else sender() ! true
        saveStateSnapshot()
      }

    case Event(Check, metadata) =>
      if(metadata.isMonthlyNull) {
        context.parent ! Cmd(StopTag(id))
        stop()
      }else goto(Receiving) andThen { _ =>
        saveStateSnapshot()
      }

    case Event(RevivalCheck, _) =>
      if(stateData.asInstanceOf[Metadata].monthlyAlreadyRun.isEmpty) {
        goto(Receiving)

      }else if(freq == "M" && stateData.asInstanceOf[Metadata].monthlyAlreadyRun.get == getLastMonth) {
        logger.info(s"Tag($freq) ID[$id] is already run in Month[${stateData.asInstanceOf[Metadata].monthlyAlreadyRun}].")
        context.parent ! Cmd(StopTag(id))
        stop()

      }else {
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

    case Event(Revival, _) =>
      goto(Receiving)

    case Event(Requirement(frequency, tmSet), _) =>
      println(s"Requirement not receive in State: $stateName")
      sender() ! false
      stay()

    case Event(Timeout(time), _) =>
      stay()
  }

  onTransition {
    case Running -> Running =>
      self ! Revival
  }

}
