package com.cathay.ddt.ats

import java.io.File

import akka.persistence._
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import com.cathay.ddt.kafka.MessageConsumer
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}
import com.cathay.ddt.tagging.schema.TagMessage.Message
import reactivemongo.bson.BSONDocument
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Tse-En on 2017/12/17.
  */

object TagManager {
  // TagManager State Operation
  sealed trait ManagerCommand
  case class Load(doc: TagDictionary) extends ManagerCommand
  case class Register(doc: TagDictionary) extends ManagerCommand
  case class Remove(doc: TagDictionary) extends ManagerCommand


  sealed trait ManagerOperation
  //sealed trait TIOperation extends ManagerOperation
  case class TagRegister(id: String) extends ManagerOperation
  case class TagMesAdded(id: String, tagMessage: Message) extends ManagerOperation
  case class TagInsRemoved(id: String) extends ManagerOperation

//  sealed trait TMOperation extends ManagerOperation
//  case class TagMesAdded(id: String, tagMessage: TagMessage) extends TMOperation
//  case class TagMesRemoved(id: String) extends TMOperation

  case class Cmd(op: ManagerCommand)
  case class Evt(op: ManagerOperation)


  case class TagInstance(id: String, actor: Option[ActorRef]=None) {
    def isActive: Boolean = actor.isDefined
  }
  case class TIsRegistry(state: Map[TagInstance, Set[Message]] = Map()) {
    def count: Int = state.size

    def register(id: String): TIsRegistry = TIsRegistry(state + (TagInstance(id) -> Set()))

    def hasRegistered(id: String): Boolean = {
      val tagIns = getTagIns(id).orNull
      if (tagIns == null) false
      else state.contains(tagIns)
    }

    def getTagIns(id: String): Option[TagInstance] = {
      //      val tagIns = state.keySet.filter( x => x.id == id )
      //      tagIns.headOption
      state.keySet.find(tagIns => tagIns.id == id)
    }

    def add(id: String, tagMessage: Message): TIsRegistry = {
      val tagIns = getTagIns(id).get
      val newSet = state(tagIns) ++ Set(tagMessage)
      TIsRegistry(state + (tagIns -> newSet))
//      (state - tagIns) + (tagIns -> newSet)
    }

    def remove(id: String) = getTagIns(id).getOrElse("error")

  }

  case class TMsRegistry(state: Map[Message, Set[TagInstance]] = Map()) {
    def add(tagMessage: Message, tagInst: TagInstance): TMsRegistry = {
      if(!state.contains(tagMessage)) {
        TMsRegistry(state + (tagMessage -> Set(tagInst)))
      } else {
        val newSet = state(tagMessage) ++ Set(tagInst)
        TMsRegistry(state + (tagMessage -> newSet))
      }
    }
  }

  case class State(tagInstReg: TIsRegistry, tagMesReg: TMsRegistry) {
    def register(id: String): State = State(tagInstReg.register(id), tagMesReg)
    def hasRegistered(tagDic: TagDictionary): Boolean = tagInstReg.hasRegistered(tagDic.actorID)
    def addTagMes(id: String, tagMessage: Message): State =
      State(
        tagInstReg.add(id, tagMessage),
        tagMesReg.add(tagMessage, tagInstReg.getTagIns(id).get)
      )
  }



  def initiate(tagClientConf: Config): ActorRef = {
    val system = ActorSystem("tag")
    system.actorOf(Props(new TagManager(tagClientConf)), name="tag-manager")
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

}

class TagManager(tagClientConf: Config) extends PersistentActor with ActorLogging {
  import TagManager._

  var state: State = State(TIsRegistry(), TMsRegistry())

//  val kafkaActor = context.actorOf(Props(new MessageConsumer(tagClientConf)), name = "")
  val config = ConfigFactory.parseFile(new File(s"config/tag.conf"))
  val kafkaActor = context.actorOf(Props(new MessageConsumer(config)), "kafka-test")


  val DATABASE = "tag"
  val SCORETAG_COLLECTION = "scoretag"
  val ICUSTOMER_COLLECTION = "icustomer"

  val connection = MongoConnector.connection
  val tagCollection = MongoConnector.dbFromConnection(connection, DATABASE, SCORETAG_COLLECTION)
  val iCustCollection = MongoConnector.dbFromConnection(connection, DATABASE, ICUSTOMER_COLLECTION)

  override def persistenceId: String = "tag-manager"

  def createActor(actorId: String): ActorRef = {
    context.actorOf(Props[Account], name = actorId)
  }

  def updateState(evt: Evt): Unit = evt match {
    case Evt(TagRegister(id)) =>
      state = state.register(id)
      //takeSnapshot
    case Evt(TagMesAdded(id, message)) =>
      state = state.addTagMes(id, message)
      //takeSnapshot
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
      println(s"Recovery Complete and Now I'll swtich to receiving mode :)")

  }

  // Persistent receive on normal mood
  val receiveCommand: Receive = {
    case cmd @ Cmd(Load(tagDic)) =>
      // println(s"TagManager receive $cmd")
      if (!state.hasRegistered(tagDic)) {
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
      } else {
        println(s"${tagDic._id} is already exist.")
      }

    case message: TagMessage  =>
      println(message)
      println(message.getDefaultTM)
      val a = context.actorOf(Props[Account])


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
