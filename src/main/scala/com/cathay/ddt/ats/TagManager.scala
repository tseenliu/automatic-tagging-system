package com.cathay.ddt.ats

import akka.persistence._
import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.kafka.MessageConsumer
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}
import com.cathay.ddt.tagging.schema.TagMessage.Message
import reactivemongo.bson.BSONDocument
import com.cathay.ddt.ats.TagState._
import akka.pattern.ask
import akka.util.Timeout
import com.cathay.ddt.utils.{EnvLoader, MessageConverter}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by Tse-En on 2017/12/17.
  */

object TagManager extends EnvLoader {

  def initiate: ActorRef = {
    val system = ActorSystem("tag")
    val tagManager = system.actorOf(Props[TagManager], name="tag-manager")
    system.actorOf(Props[TagScheduler], name="tag-scheduler")
    initialDictionary(tagManager)

    // if not test, should delete
    tagManager
  }

  def initialDictionary(tagManager: ActorRef): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    //     load customer dictionary
    // "disable_flag" -> false
    val query = BSONDocument()
    MongoConnector.getTDCollection.flatMap(tagColl => MongoUtils.findDictionaries(tagColl, query)).map { docList =>
      docList.foreach(TD => tagManager ! Cmd(Load(TD)))
    }
  }

  // TagManager State Operation
  sealed trait ManagerCommand
  case class Load(doc: TagDictionary) extends ManagerCommand
  case class Register(doc: TagDictionary) extends ManagerCommand
  case class StopTag(id: String) extends ManagerCommand
  case class Remove(id: String) extends ManagerCommand
  case class Update(doc: TagDictionary) extends ManagerCommand

  case object ShowState extends ManagerCommand


  sealed trait ManagerOperation
  //sealed trait TIOperation extends ManagerOperation
  case class TagRegister(tagDic: TagDictionary) extends ManagerOperation
  case class TagMesAdded(id: String, tagMessage: Message) extends ManagerOperation
  case class TagInsActorCreated(ti: TagInstance, actorRef: ActorRef) extends ManagerOperation
  case class TagInsStopped(id: String) extends ManagerOperation
  case class TagInsRemoved(id: String) extends ManagerOperation
  case class TagInsUpdated(id: String, tagMessage: Set[Message]) extends ManagerOperation


  case class Cmd(op: ManagerCommand)
  case class Evt(op: ManagerOperation)


  case class TagInstance(frequency: String, id: String, actor: Option[ActorRef]=None) {
    def isActive: Boolean = actor.isDefined
  }

  case class TIsRegistry(state: Map[TagInstance, Set[Message]] = Map()) {
    def count: Int = state.size
    def getTMs(ti: TagInstance): Set[Message] = state(ti)
    def getTIs: Set[TagInstance] = state.keySet
    def register(tagDic: TagDictionary): TIsRegistry =
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

    def updateAdd(id: String, tagMessages: Set[Message]): TIsRegistry = {
      val tagIns = getTI(id).get
      TIsRegistry(state + (tagIns -> tagMessages))
    }

    def update(oldTI: TagInstance, newTI: TagInstance): TIsRegistry = {
      val messageSet = state(oldTI)
      val newState = state - oldTI + (newTI -> messageSet)
      TIsRegistry(newState)
    }

    def stop(id: String): TIsRegistry = {
      val tagIns = getTI(id).get
      val dTagIns = TagInstance(tagIns.frequency.toUpperCase(), tagIns.id)
      val messageSet = state(tagIns)
      val newState = state - tagIns + (dTagIns -> messageSet)
      TIsRegistry(newState)
    }

    def remove(id: String): TIsRegistry = {
      val tagIns = getTI(id).get
      val newState = state - tagIns
      TIsRegistry(newState)
    }

    def getNumsOfTags: Int = {
      state.size
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

    def updateAdd(tagMessages: Iterator[Message], tagInst: TagInstance): TMsRegistry = {
      var newState: Map[Message, Set[TagInstance]] = state
      while (tagMessages.hasNext){
        val value = tagMessages.next()
        if(!state.contains(value)) {
          newState += (value -> Set(tagInst))
        } else {
          val newSet = state(value) ++ Set(tagInst)
          newState += (value -> newSet)
        }
      }
      TMsRegistry(newState)
    }

    def update(messages: Set[Message], oldTI: TagInstance, newTI: TagInstance): TMsRegistry = {
      var newState: Map[Message, Set[TagInstance]] = state
      messages.foldLeft(this){ (registry, mes) =>
        val newSet = newState(mes) - oldTI + newTI
        newState = newState + (mes -> newSet)
        TMsRegistry(newState)
      }
    }

    def removeUpdate(messages: Set[Message], oldTI: TagInstance): TMsRegistry = {
      var newState: Map[Message, Set[TagInstance]] = state
      messages.foldLeft(this){ (registry, mes) =>
        val newSet = newState(mes) - oldTI
        if(newSet.isEmpty) newState = newState - mes
        else newState = newState + (mes -> newSet)
        TMsRegistry(newState)
      }
    }

    def getNumsOfTables: Int = {
      state.size
    }
  }

  case class State(tagInstReg: TIsRegistry, tagMesReg: TMsRegistry) {
    def register(tagDic: TagDictionary): State = State(tagInstReg.register(tagDic), tagMesReg)
    def contains(tagDic: TagDictionary): Boolean = tagInstReg.contains(tagDic.actorID)
    def contains(message: Message): Boolean = tagMesReg.contains(message)
    def getTIs(message: Message): Set[TagInstance] = tagMesReg.getTIs(message)
    def getTIs: Set[TagInstance] = tagInstReg.getTIs
    def getTMs(ti: TagInstance): Set[Message] = tagInstReg.getTMs(ti)
    def stop(id: String): State = {
      val newTagInsReg = tagInstReg.stop(id)
      State(
        newTagInsReg,
        tagMesReg.update(tagInstReg.getTMs(tagInstReg.getTI(id).get), tagInstReg.getTI(id).get, newTagInsReg.getTI(id).get)
      )
    }

    def remove(id: String): State = {
      val newTagInsReg = tagInstReg.remove(id)
      State(
        newTagInsReg,
        tagMesReg.removeUpdate(tagInstReg.getTMs(tagInstReg.getTI(id).get), tagInstReg.getTI(id).get)
      )
    }

    def add(id: String, tagMessage: Message): State =
      State(
        tagInstReg.add(id, tagMessage),
        tagMesReg.add(tagMessage, tagInstReg.getTI(id).get)
      )

    def createActor(oldTI: TagInstance, actor: ActorRef): State = {
      val newTI = TagInstance(oldTI.frequency.toUpperCase(), oldTI.id, Some(actor))
      State(
        tagInstReg.update(oldTI, newTI),
        tagMesReg.update(tagInstReg.getTMs(oldTI), oldTI, newTI))
    }

    def update(id: String, tagMessages: Set[Message]): State = {
      val removeTagMesReg = tagMesReg.removeUpdate(tagInstReg.getTMs(tagInstReg.getTI(id).get), tagInstReg.getTI(id).get)
      State(
        tagInstReg.updateAdd(id, tagMessages),
        removeTagMesReg.updateAdd(tagMessages.toIterator, tagInstReg.getTI(id).get)
      )
    }

    def initActors(createActor: (String, String) => ActorRef): State = {
      val registry = getTIs.foldLeft(this) { (state, ti) =>
        if (ti.isActive) {
          val actorRef = createActor(ti.frequency, ti.id)
          state.createActor(ti, actorRef)
        } else state
      }
      registry
    }

    def ShowTagInfo(): Unit = {
      println(s"===================================================================" +
        s"\n[Info] Tag Dictionary loading finished." +
        s"\nTotal Tags:${tagInstReg.getNumsOfTags}" +
        s"\nTotal Tables:${tagMesReg.getNumsOfTables}\n")
    }
  }

}

class TagManager extends PersistentActor with ActorLogging {
  import TagManager._

  override def preStart(): Unit = {
    println(s"[Info] ${self}: TagManager is [Start].")
  }

  override def postStop(): Unit = {
    println(s"[Info] ${self}: TagManager is [Stop].")
  }

  var state: State = State(TIsRegistry(), TMsRegistry())
  context.actorOf(Props[MessageConsumer], "messages-consumer")

  override def persistenceId: String = "tag-manager"

  def createActor(frequency: String, actorId: String): ActorRef = {
    context.actorOf(Props(new TagState(frequency, actorId)) ,name = actorId)
  }

  def updateState(evt: Evt): Unit = evt match {
    case Evt(TagRegister(tagDic)) =>
      state = state.register(tagDic)
      saveSnapshot(state)
    case Evt(TagMesAdded(id, message)) =>
      state = state.add(id, message)
      saveSnapshot(state)
    case Evt(TagInsActorCreated(ti, actorRef)) =>
      state = state.createActor(ti, actorRef)
      saveSnapshot(state)
    case Evt(TagInsStopped(id)) =>
      state = state.stop(id)
      saveSnapshot(state)
    case Evt(TagInsRemoved(id)) =>
      state = state.remove(id)
      saveSnapshot(state)
    case Evt(TagInsUpdated(id, tms)) =>
      state = state.update(id, tms)
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
    persist(Evt(TagInsActorCreated(ti, actorRef))) { evt =>
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
        println(s"tagID: ${tagDic.actorID} is already exist.")
      }

    case cmd @ Cmd(StopTag(id)) =>
      persist(Evt(TagInsStopped(id))) { evt =>
        updateState(evt)
      }

    case cmd @ Cmd(Remove(id)) =>
      persist(Evt(TagInsRemoved(id))) { evt =>
        updateState(evt)
      }

    case cmd @ Cmd(Update(tagDic)) =>
      val tagMessages = MessageConverter.getMessages(tagDic.sql).toSet
      persist(Evt(TagInsUpdated(tagDic.tag_id, tagMessages))) { evt =>
        updateState(evt)
      }
      val ti = state.tagInstReg.getTI(tagDic.tag_id)
      if(ti.get.isActive) {
        ti.get.actor.get ! Requirement(tagMessages)
      }

    case tagMessage: TagMessage  =>
      val message = tagMessage.getDefaultTM
      if (state.contains(message)) {
        state.getTIs(message).foreach { ti =>
          if (ti.isActive) ti.actor.get ! Receipt(tagMessage)
          else createAndSend(ti, tagMessage)
        }
      }

    case Cmd(ShowState) =>
      state.ShowTagInfo()
//      println(s"The Current state of counter is $state")

    case SaveSnapshotSuccess(metadata) =>
//      println(s"save snapshot succeed.")
    case SaveSnapshotFailure(metadata, reason) =>
      println(s"save snapshot failed and failure is $reason")

  }



}
