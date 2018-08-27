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
import org.slf4j.LoggerFactory

/**
  * Created by Tse-En on 2017/12/23.
  */
class MessageConsumer extends Actor with EnvLoader {

  val log = LoggerFactory.getLogger(this.getClass)
  override def preStart(): Unit = {
    log.info(s"MessageConsumer is [Start].")
  }

  override def postStop(): Unit = {
    log.info(s"MessageConsumer is [Stop].")
  }

  private val kafkaConfig: Config = getConfig("kafka")
  private val consumerConf = kafkaConfig.getConfig("kafka.consumer")
  private val subscribeTopics: Array[String] = kafkaConfig.getStringList("tag.subscribe-topics").toArray().map(_.toString)
  private val publishTopic = kafkaConfig.getString("tag.finishmsg-topic")

  val subscribe = subscribeTopics.toSet
//  val frontier = subscribeTopics.toSet -- Set(publishTopic)

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
    case Terminated(watchActor) => log.error(s"${watchActor.path} to be killed.")

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
          case m if subscribe.contains(m) =>
            log.info(s"MessageConsumer is received: ${r.value()} from [$m] topic.")
            val message: FinishMessage = r.value().parseJson.convertTo[FinishMessage]
            if(message.is_success) {
              val tagMessage = MessageConverter.CovertToTM(r.topic(), message)
              context.parent ! tagMessage
            }
        }
      } catch {
        case e: Exception =>
          println(e)
          log.error(s"${r.value()} not a correct format")
      }
    }
  }

}
