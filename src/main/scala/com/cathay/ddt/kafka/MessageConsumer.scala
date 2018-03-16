package com.cathay.ddt.kafka

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import cakesolutions.kafka.akka.{ConsumerRecords, Extractor, KafkaConsumerActor}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import spray.json._
import TagJsonProtocol._
import com.cathay.ddt.utils.{EnvLoader, MessageConverter}

/**
  * Created by Tse-En on 2017/12/23.
  */
class MessageConsumer extends Actor with ActorLogging with EnvLoader {
  override def preStart(): Unit = {
    println(s"[Info] ${self}: MessageConsumer is [Start].")
  }

  override def postStop(): Unit = {
    println(s"[Info] ${self}: MessageConsumer is [Stop].")
  }

  private val kafkaConfig: Config = getConfig("kafka")
  private val consumerConf = kafkaConfig.getConfig("kafka.consumer")
  private val subscribeTopics: Array[String] = kafkaConfig.getStringList("tag.subscribe-topics").toArray().map(_.toString)
  private val publishTopic = kafkaConfig.getString("tag.publish-topic")

  val frontier = subscribeTopics.toSet -- Set(publishTopic)

  // Records' type of [key, value]
  val recordsExt: Extractor[Any, ConsumerRecords[String, String]] = ConsumerRecords.extractor[String, String]

  val consumer: ActorRef = context.actorOf(
    KafkaConsumerActor.props(
      consumerConf,
      new StringDeserializer,
      new StringDeserializer,
      self
    ), "Tag-Reporter"
  )
  consumer ! Subscribe.AutoPartition(subscribeTopics)
  context.watch(consumer)

  override def receive: Receive = {
    case Terminated(watchActor) => println(s"[ERROR] ${watchActor.path} to be killed.")

    // Records from Kafka
    case recordsExt(records) =>
      sender ! Confirm(records.offsets, commit = true)
      processRecords(records.recordsList)
  }

  //override def to customize process value of consumer
  protected def processRecords(recordsList: List[ConsumerRecord[String, String]]): Unit = {
    recordsList.foreach { r =>
      try {
        // Parse records in Json format
        r.topic() match {
          case m if frontier.contains(m) =>
            println(s"[Info] MessageConsumer is received: ${r.value()} from [$m] topic.")
            val message: FrontierMessage = r.value().parseJson.convertTo[FrontierMessage]
            val tagMessage = MessageConverter.CovertToTM(r.topic(), message)
            context.parent ! tagMessage
          case m if m == publishTopic =>
            println(s"[Info] MessageConsumer is received: ${r.value()} from [$m] topic.")
            val message: TagFinishMessage = r.value().parseJson.convertTo[TagFinishMessage]
            val tagMessage = MessageConverter.CovertToTM(r.topic(), message)
            context.parent ! message
        }
      } catch {
        case e: Exception =>
          println(e)
          log.error(s"${r.value()} not a correct format")
      }
    }
  }

}
