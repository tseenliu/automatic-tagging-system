package com.cathay.ddt.ats

import akka.persistence._
import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.kafka.MessageConsumer
import com.cathay.ddt.tagging.schema.{CustomerDictionary, TagMessage}
import com.cathay.ddt.tagging.schema.TagMessage.Message
import reactivemongo.bson.BSONDocument
import com.typesafe.config.Config
import com.cathay.ddt.ats.TagState._
import akka.pattern.ask
import akka.util.Timeout
import com.cathay.ddt.utils.{EnvLoader, MessageConverter}

import scala.concurrent.duration._

/**
  * Created by Tse-En on 2017/12/17.
  */

object TagManager extends EnvLoader {

  def initiate(kafkaConfig: Config): ActorRef = {
    val system = ActorSystem("tag")
    val tagManager = system.actorOf(Props(new TagManager(kafkaConfig)), name="tag-manager")
    initialDictionary(tagManager)
    tagManager
  }

  def initialDictionary(tagManager: ActorRef) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    //     load customer dictionary
    val query = BSONDocument("attribute" -> "behavior", "enable_flag" -> true)
    MongoConnector.getCDCollection.flatMap(tagColl => MongoUtils.findDictionaries(tagColl, query)).map { docList =>
      docList.foreach(CD => tagManager ! Cmd(Load(CD)))
    }
  }

  // TagManager State Operation
  sealed trait ManagerCommand
  case class Load(doc: CustomerDictionary) extends ManagerCommand
  case class Register(doc: CustomerDictionary) extends ManagerCommand
  case class Delete(id: String) extends ManagerCommand
  case class Remove(id: String) extends ManagerCommand

  case object ShowState extends ManagerCommand


  sealed trait ManagerOperation
  //sealed trait TIOperation extends ManagerOperation
  case class TagRegister(tagDic: CustomerDictionary) extends ManagerOperation
  case class TagMesAdded(id: String, tagMessage: Message) extends ManagerOperation
  case class TagMesUpdated(ti: TagInstance, actorRef: ActorRef) extends ManagerOperation
  case class TagInsDelete(id: String) extends ManagerOperation
  case class TagInsRemoved(id: String) extends ManagerOperation


  case class Cmd(op: ManagerCommand)
  case class Evt(op: ManagerOperation)


  case class TagInstance(frequency: String, id: String, actor: Option[ActorRef]=None) {
    def isActive: Boolean = actor.isDefined
  }

  case class TIsRegistry(state: Map[TagInstance, Set[Message]] = Map()) {
    def count: Int = state.size
    def getTMs(ti: TagInstance): Set[Message] = state(ti)
    def getTIs: Set[TagInstance] = state.keySet
    def register(tagDic: CustomerDictionary): TIsRegistry =
      TIsRegistry(state + (TagInstance(tagDic.update_frequency.toUpperCase, tagDic.actorID) -> Set()))

    def getTI(id: String): Option[TagInstance] = {
      state.keySet.find(tagIns => tagIns.id == id)
    }

    def contains(id: String): Boolean = {
      val tagIns = getTI(id).orNull
      if (tagIns == null) false
      else state.contains(tagIns)
    }

    def add(id: String, tagMessage: Message): TIsRegistry = {
      val tagIns = getTI(id).get
      val messageSet = state(tagIns) ++ Set(tagMessage)
      TIsRegistry(state + (tagIns -> messageSet))
    }

    def update(oldTI: TagInstance, newTI: TagInstance): TIsRegistry = {
      val messageSet = state(oldTI)
      val newState = state - oldTI + (newTI -> messageSet)
      TIsRegistry(newState)
    }

    def delete(id: String): TIsRegistry = {
      val tagIns = getTI(id).get
      val dTagIns = TagInstance(tagIns.frequency, tagIns.id)
      val messageSet = state(tagIns)
      val newState = state - tagIns + (dTagIns -> messageSet)
      TIsRegistry(newState)
    }
  }

  case class TMsRegistry(state: Map[Message, Set[TagInstance]] = Map()) {
    def contains(message: Message): Boolean = state.contains(message)

    def getTIs(message: Message): Set[TagInstance] = state(message)

    def add(tagMessage: Message, tagInst: TagInstance): TMsRegistry = {
      if(!state.contains(tagMessage)) {
        TMsRegistry(state + (tagMessage -> Set(tagInst)))
      } else {
        val newSet = state(tagMessage) ++ Set(tagInst)
        TMsRegistry(state + (tagMessage -> newSet))
      }
    }

    def update(messages: Set[Message], oldTI: TagInstance, newTI: TagInstance): TMsRegistry = {
      var newState: Map[Message, Set[TagInstance]] = state
      messages.foldLeft(this){ (registry, mes) =>
        val newSet = newState(mes) - oldTI + newTI
        newState = newState + (mes -> newSet)
        TMsRegistry(newState)
      }
    }
  }

  case class State(tagInstReg: TIsRegistry, tagMesReg: TMsRegistry) {
    def register(tagDic: CustomerDictionary): State = State(tagInstReg.register(tagDic), tagMesReg)
    def contains(tagDic: CustomerDictionary): Boolean = tagInstReg.contains(tagDic.actorID)
    def contains(message: Message): Boolean = tagMesReg.contains(message)
    def getTIs(message: Message): Set[TagInstance] = tagMesReg.getTIs(message)
    def getTIs: Set[TagInstance] = tagInstReg.getTIs
    def getTMs(ti: TagInstance): Set[Message] = tagInstReg.getTMs(ti)
    def delete(id: String): State = {
      val newTagInsReg = tagInstReg.delete(id)
      State(
        newTagInsReg,
        tagMesReg.update(tagInstReg.getTMs(tagInstReg.getTI(id).get), tagInstReg.getTI(id).get, newTagInsReg.getTI(id).get)
      )
    }

    def add(id: String, tagMessage: Message): State =
      State(
        tagInstReg.add(id, tagMessage),
        tagMesReg.add(tagMessage, tagInstReg.getTI(id).get)
      )

    def update(oldTI: TagInstance, actor: ActorRef): State = {
      val newTI = TagInstance(oldTI.frequency, oldTI.id, Some(actor))
      State(
        tagInstReg.update(oldTI, newTI),
        tagMesReg.update(tagInstReg.getTMs(oldTI), oldTI, newTI))
    }

    def initActors(createActor: (String, String) => ActorRef): State = {
      val registry = getTIs.foldLeft(this) { (state, ti) =>
        if (ti.isActive) {
          val actorRef = createActor(ti.frequency, ti.id)
          state.update(ti, actorRef)
        } else state
      }
      registry
    }
  }

}

class TagManager(kafkaConfig: Config) extends PersistentActor with ActorLogging {
  import TagManager._

  var state: State = State(TIsRegistry(), TMsRegistry())
  val kafkaActor = context.actorOf(Props(new MessageConsumer(kafkaConfig)), "messages-consumer")

  override def persistenceId: String = "tag-manager"

  def createActor(frequency: String, actorId: String): ActorRef = {
    context.actorOf(Props(new TagState(frequency, actorId)) ,name = actorId)
  }

  def updateState(evt: Evt): Unit = evt match {
    case Evt(TagRegister(tagDic)) =>
      state = state.register(tagDic)
    case Evt(TagMesAdded(id, message)) =>
      state = state.add(id, message)
      saveSnapshot(state)
    case Evt(TagMesUpdated(ti, actorRef)) =>
      state = state.update(ti, actorRef)
      saveSnapshot(state)
    case Evt(TagInsDelete(ti)) =>
      state = state.delete(ti)
      saveSnapshot(state)
  }

  def createAndSend(ti: TagInstance, tagMessage: TagMessage) = {
    implicit val timeout = Timeout(5 seconds)
    import scala.concurrent.ExecutionContext.Implicits.global
    val actorRef = context.actorOf(Props(new TagState(ti.frequency, ti.id)) ,name = ti.id)
    (actorRef ? Requirement(state.getTMs(ti))).map{
      case true =>
        actorRef ! Receipt(tagMessage)
      case false =>
        println("Initial require messages error.")
    }
    persist(Evt(TagMesUpdated(ti, actorRef))) { evt =>
      updateState(evt)
    }
  }

  // Persistent receive on recovery mood
  val receiveRecover: Receive = {
    case evt: Evt =>
      println(s"Counter receive $evt on recovering mood")
      updateState(evt)
    case SnapshotOffer(_, snapshot: State) =>
      println(s"Counter receive snapshot with data: $snapshot on recovering mood")
      state = snapshot
    case RecoveryCompleted =>
      state = state.initActors(createActor)
      println(s"Recovery Complete and Now I'll swtich to receiving mode :)")

  }

  // Persistent receive on normal mood
  val receiveCommand: Receive = {
    case cmd @ Cmd(Load(tagDic)) =>
      if (!state.contains(tagDic)) {
        // register
        persist(Evt(TagRegister(tagDic))) { evt =>
          updateState(evt)
        }
        // add tag require messages
        val tagMessages = MessageConverter.getMessages(tagDic.sql)
        while (tagMessages.hasNext){
          persist(Evt(TagMesAdded(tagDic.actorID, tagMessages.next()))) { evt =>
            updateState(evt)
          }
        }
      } else{
        println(s"${tagDic._id} is already exist.")
      }

    case tagMessage: TagMessage  =>
      val message = tagMessage.getDefaultTM
      if (state.contains(message)) {
        state.getTIs(message).foreach { ti =>
          if (ti.isActive) ti.actor.get ! Receipt(tagMessage)
          else createAndSend(ti, tagMessage)
        }
      }

    case cmd @ Cmd(Delete(id))  =>
      persist(Evt(TagInsDelete(id))) { evt =>
        updateState(evt)
      }

    case Cmd(ShowState) =>
      println(s"The Current state of counter is $state")

    case SaveSnapshotSuccess(metadata) =>
      println(s"save snapshot succeed.")
    case SaveSnapshotFailure(metadata, reason) =>
      println(s"save snapshot failed and failure is $reason")

  }



}
