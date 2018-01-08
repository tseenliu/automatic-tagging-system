package com.cathay.ddt.ats

import akka.persistence._
import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.kafka.MessageConsumer
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}
import com.cathay.ddt.tagging.schema.TagMessage.Message

import reactivemongo.bson.BSONDocument
import com.typesafe.config.Config

import com.cathay.ddt.ats.Account.{CR, DR, Operation}

/**
  * Created by Tse-En on 2017/12/17.
  */

object TagManager {

  def initiate(kafkaConfig: Config): ActorRef = {
    val system = ActorSystem("tag")
    system.actorOf(Props(new TagManager(kafkaConfig)), name="tag-manager")
  }

  def exportToRegistries(tagManager: ActorRef) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val connection1 = MongoConnector.connection
    val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag")
    val query = BSONDocument("attribute" -> "behavior")
    FBsonCollection.flatMap(scoreTagColl => MongoUtils.getScoreTDs(scoreTagColl, query)).map { docList =>
      for (doc <- docList) {
        //println(doc)
        tagManager ! Cmd(Load(doc))
      }
    }
  }

  // TagManager State Operation
  sealed trait ManagerCommand
  case class Load(doc: TagDictionary) extends ManagerCommand
  case class Register(doc: TagDictionary) extends ManagerCommand
  case class Remove(doc: TagDictionary) extends ManagerCommand


  sealed trait ManagerOperation
  //sealed trait TIOperation extends ManagerOperation
  case class TagRegister(id: String) extends ManagerOperation
  case class TagMesAdded(id: String, tagMessage: Message) extends ManagerOperation
  case class TagMesUpdated(ti: TagInstance, actorRef: ActorRef) extends ManagerOperation
  case class TagInsRemoved(id: String) extends ManagerOperation

  case class Cmd(op: ManagerCommand)
  case class Evt(op: ManagerOperation)


  case class TagInstance(id: String, actor: Option[ActorRef]=None) {
    def isActive: Boolean = actor.isDefined
  }

  case class TIsRegistry(state: Map[TagInstance, Set[Message]] = Map()) {
    def count: Int = state.size
    def getTMs(ti: TagInstance): Set[Message] = state(ti)
    def getTIs: Set[TagInstance] = state.keySet
    def register(id: String): TIsRegistry = TIsRegistry(state + (TagInstance(id) -> Set()))

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
      val newSet = state(tagIns) ++ Set(tagMessage)
      TIsRegistry(state + (tagIns -> newSet))
    }

    def update(oldTI: TagInstance, newTI: TagInstance): TIsRegistry = {
      val messageSet = state(oldTI)
      val newState = state - oldTI + (newTI -> messageSet)
      TIsRegistry(newState)
    }



    // test
    def remove(id: String) = getTI(id).getOrElse("error")
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
    def register(id: String): State = State(tagInstReg.register(id), tagMesReg)
    def contains(tagDic: TagDictionary): Boolean = tagInstReg.contains(tagDic.actorID)
    def contains(message: Message): Boolean = tagMesReg.contains(message)
    def getTIs(message: Message): Set[TagInstance] = tagMesReg.getTIs(message)
    def getTIs: Set[TagInstance] = tagInstReg.getTIs
    def add(id: String, tagMessage: Message): State =
      State(
        tagInstReg.add(id, tagMessage),
        tagMesReg.add(tagMessage, tagInstReg.getTI(id).get)
      )

    def update(oldTI: TagInstance, actor: ActorRef): State = {
      val newTI = TagInstance(oldTI.id, Some(actor))
      State(
        tagInstReg.update(oldTI, newTI),
        tagMesReg.update(tagInstReg.getTMs(oldTI), oldTI, newTI))
    }

    def initActors(createActor: String => ActorRef): State = {
      val registry = getTIs.foldLeft(this) { (state, ti) =>
        if (ti.isActive) {
          val actorRef = createActor(ti.id)
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
  val kafkaActor = context.actorOf(Props(new MessageConsumer(kafkaConfig)), "kafka-test")

  override def persistenceId: String = "tag-manager"

  def createActor(actorId: String): ActorRef = {
    context.actorOf(Props(new Account(actorId)) ,name = actorId)
  }

  def updateState(evt: Evt): Unit = evt match {
    case Evt(TagRegister(id)) =>
      state = state.register(id)
    //takeSnapshot
    case Evt(TagMesAdded(id, message)) =>
      state = state.add(id, message)
    //takeSnapshot
    case Evt(TagMesUpdated(ti, actorRef)) =>
      state = state.update(ti, actorRef)
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
        persist(Evt(TagRegister(tagDic.actorID))) { evt =>
          updateState(evt)
        }
        // add tag require messages
        val tagMessages = SqlParser.getTagMessages(tagDic.sql)
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
          if (ti.isActive) {
            ti.actor.get ! Operation(1000, CR)
          } else{
            val actorRef = context.actorOf(Props(new Account(ti.id)) ,name = ti.id)
            actorRef ! Operation(1000, CR)
            //            state.update(ti, actorRef)
            persist(Evt(TagMesUpdated(ti, actorRef))) { evt =>
              updateState(evt)
            }
          }

        }
      }

    case "print" =>
      saveSnapshot(state)
      println(s"The Current state of counter is $state")

    case SaveSnapshotSuccess(metadata) =>
      println(s"save snapshot succeed.")
    case SaveSnapshotFailure(metadata, reason) =>
      println(s"save snapshot failed and failure is $reason")

  }

  def takeSnapshot = {
    if(state.tagInstReg.count % 3 == 0){
      saveSnapshot(state)
    }
  }
}
