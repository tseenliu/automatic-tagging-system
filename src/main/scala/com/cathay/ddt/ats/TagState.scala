package com.cathay.ddt.ats

import scala.reflect._
import akka.persistence._
import akka.persistence.fsm._
import akka.persistence.fsm.PersistentFSM.FSMState
import com.cathay.ddt.ats.TagManager.{Cmd, Delete}
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import com.cathay.ddt.utils.CalendarConverter
import reactivemongo.bson.{BSONDocument, BSONObjectID}

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
    val daily: Map[String, Boolean]
    val monthly: Map[String, Boolean]
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
  }

  case object ZeroMetadata extends Data {
    override val daily: Map[String, Boolean] = Map()
    override val monthly: Map[String, Boolean] = Map()
  }

  case class Metadata(override val daily: Map[String, Boolean], override val monthly: Map[String, Boolean]) extends Data {
    override def toString: String = {
      s"daily: $daily\nmonthly: $monthly"
    }
  }


  // Domain Events (Persist events)
  sealed trait DomainEvent

  case class RequiredMessages(requireTMs: Set[Message]) extends DomainEvent

  case class ReceivedMessage(tagMessage: TagMessage, `type`: FrequencyType) extends DomainEvent

  case class UpdatedMessages(`type`: FrequencyType) extends DomainEvent


  // Frequency Types
  sealed trait FrequencyType

  case object Daily extends FrequencyType

  case object Monthly extends FrequencyType



  // Commands
  case class Requirement(requireTMs: Set[Message])
  case class Receipt(tagMessage: TagMessage)
  case object Check
  case object Launch
  case object Stop

}

class TagState(frequency: String, id: String) extends PersistentFSM[TagState.State, TagState.Data, TagState.DomainEvent] with CalendarConverter {
  import TagState._

  override def persistenceId: String = id

  override def applyEvent(evt: DomainEvent, currentData: Data): Data = {

    def resetDaily: Map[String, Boolean] = {
      val daily = collection.mutable.Map[String, Boolean]()
      currentData.daily.keys.foreach( k=> daily += (k -> false) )
      daily.toMap
    }

    def resetMonthly: Map[String, Boolean] = {
      val monthly = collection.mutable.Map[String, Boolean]()
      currentData.monthly.keys.foreach( k=> monthly += (k -> false) )
      monthly.toMap
    }

    evt match {
      case RequiredMessages(requireTMs: Set[Message]) =>
        val messages = requireTMs.asInstanceOf[Set[SimpleTagMessage]]
        val daily = collection.mutable.Map[String, Boolean]()
        val monthly = collection.mutable.Map[String, Boolean]()
        messages.foreach { m =>
          m.update_frequency match {
            case "M" => monthly += (m.value -> currentData.monthly.getOrElse(m.value, false))
            case "D" => daily += (m.value -> currentData.daily.getOrElse(m.value, false))
          }
        }
        Metadata(daily.toMap, monthly.toMap)

      case ReceivedMessage(tm, Daily) =>
        // t-1 month, t-2 day -> true
        tm.kafkaTopic match {
          case "frontier-adw" =>
            if(tm.yyyymmdd.contains(getDailyDate)) {
              Metadata(currentData.daily + (tm.value -> true), currentData.monthly)
            }else if(tm.yyyymm == tm.yyyymmdd){
              Metadata(currentData.daily + (tm.value -> true), currentData.monthly)
            } else {
              currentData
            }
          case "hippo-finish" =>
            if(tm.yyyymmdd.contains(getCurrentDate)) {
              Metadata(currentData.daily + (tm.value -> true), currentData.monthly)
            }else {
              currentData
            }
        }

      case ReceivedMessage(tm, Monthly) =>
        // t-1 month, t-2 day -> true
        tm.kafkaTopic match {
          case "frontier-adw" =>
            if(tm.yyyymm.contains(getLastMonth))
              Metadata(currentData.daily, currentData.monthly + (tm.value -> true))
            else currentData
          case "hippo-finish" =>
            if(tm.yyyymm.contains(getCurrentMonth))
              Metadata(currentData.daily, currentData.monthly + (tm.value -> true))
            else currentData
        }

      case UpdatedMessages(Daily) =>
        if (currentData.monthly.nonEmpty & currentData.daily.nonEmpty) {
          // if(last day) reset for next new month
          if(getCurrentDate == getDayOfMonth(numsOfDelayDate - 1)) Metadata(resetDaily, resetMonthly)
          // keep monthly reset daily
          else Metadata(resetDaily, currentData.monthly)

        }else {
          Metadata(resetDaily, resetMonthly)
        }

      case UpdatedMessages(Monthly) =>
        Metadata(resetDaily, resetMonthly)

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
        println(stateData)
      }
  }

  when(Receiving){
    case Event(Requirement(tmSet), _) =>
      stay applying RequiredMessages(tmSet) andThen { _ =>
        sender() ! true
        saveStateSnapshot()
        println(stateData)
      }

    case Event(Receipt(tm), _) =>
      tm.update_frequency match {
        case "D" =>
          stay applying ReceivedMessage(tm, Daily) andThen { _ =>
            saveStateSnapshot()
            self ! Check
            println(stateData)
        }
        case "M" =>
          stay applying ReceivedMessage(tm, Monthly) andThen{ _ =>
            saveStateSnapshot()
            self ! Check
            println(stateData)
        }
      }

    case Event(Check, _) =>
      if(stateData.isReady){
        goto(Running) andThen{ _ =>
          self ! Launch
        }
      }else {
        stay()
      }
  }

  when(Running) {
    case Event(Launch, _) =>
      frequency match {
        case "M" =>
          if(getCurrentDate == getDayOfMonth(numsOfDelayDate)) {
            // run
            // sender ! ???
            println(getComposedSql)
          }
          goto(Verifying) applying UpdatedMessages(Monthly) andThen { _ =>
            self ! Check
          }
        case "D" =>
          // run
          // or send to State scheduler
          println(getComposedSql)

          goto(Verifying) applying UpdatedMessages(Daily) andThen { _ =>
            self ! Check
          }
      }

  }

  when(Verifying){
    case Event(Check, metadata) =>
      if(metadata.isNull) {
        context.parent ! Cmd(Delete(id))
        self ! Stop
        stay()
      }else {
        goto(Receiving)
      }

    case Event(Stop, _) =>
      clearPersistentData()
      stop()

  }

  def clearPersistentData(): Unit = {
    (1L to lastSequenceNr) foreach deleteMessages
    deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = lastSequenceNr))
  }

  def getDictionary: Future[TagDictionary] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val query = BSONDocument("_id" -> BSONObjectID(id))
    MongoConnector.getTDCollection.flatMap(x => MongoUtils.findOneDictionary(x, query))
  }

  def getComposedSql: String = {
    val dic = Await.result(getDictionary, 1 second)
    val startDate = getStartWithDate(dic.started)
    val endDate = getEndWithDate(startDate, dic.traced)
    dic.sql.replaceAll("\\$start_date", startDate).replaceAll("\\$end_date", endDate)
  }

//  onTransition {
//    case Receiving -> Receiving =>
//      self ! Retry
//      if(stateData.isReady) {
//        goto(Loading)
//      }
//  }

  whenUnhandled {
    case Event(SaveSnapshotSuccess(metadata), _) ⇒
      println(stateName)
      stay()

    case Event(SaveSnapshotFailure(metadata, reason), _) ⇒
      println(s"save snapshot failed and failure is $reason")
      stay()
  }
}