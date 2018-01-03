package com.cathay.ddt.kafka

import akka.actor.{Actor, ActorLogging, Terminated}
import cakesolutions.kafka.akka.{ConsumerRecords, Extractor, KafkaConsumerActor}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import spray.json._
import TagJsonProtocol._
import com.cathay.ddt.ats.SqlParser
import com.cathay.ddt.tagging.schema.TagMessage
/**
  * Created by Tse-En on 2017/12/23.
  */
class MessageConsumer(config: Config) extends Actor with ActorLogging {

  private val consumerConf = config.getConfig("kafka.consumer")
  private val topics: Array[String] = config.getStringList("hippo.subscribe-topics").toArray().map(_.toString)

  // Records' type of [key, value]
  val recordsExt = ConsumerRecords.extractor[String, String]

  val consumer = context.actorOf(
    KafkaConsumerActor.props(
      consumerConf,
      new StringDeserializer,
      new StringDeserializer,
      self
    ), "Reporter"
  )
  consumer ! Subscribe.AutoPartition(topics)
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
        r.value() match {
          case frontierM if frontierM.contains("table") & frontierM.contains("db") =>
            println(frontierM)
            val message: FrontierMessage = frontierM.parseJson.convertTo[FrontierMessage]
            context.parent ! message
          case tagFinishM if tagFinishM.contains("hippo_name") & tagFinishM.contains("job_name") =>
            println(tagFinishM)
            val message: TagFinishMessage = tagFinishM.parseJson.convertTo[TagFinishMessage]
            context.parent ! message
          case tagM if tagM.contains("kafkaTopic") & tagM.contains("update_frequency") =>
            println(tagM)
            val message: TagMessageTest = tagM.parseJson.convertTo[TagMessageTest]
            val tagMessage = SqlParser.CovertTagMessages(r.topic(), message)
            context.parent ! tagMessage
        }
      } catch {
        case e: Exception =>
          println(e)
          log.error(s"${r.value()} not a tagging message format")
      }
    }
  }



}
