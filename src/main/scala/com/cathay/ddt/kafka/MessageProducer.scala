package com.cathay.ddt.kafka

import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import com.cathay.ddt.tagging.schema.TagDictionary
import com.cathay.ddt.utils.{CalendarConverter, EnvLoader}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._
import TagJsonProtocol._
import com.cathay.ddt.ats.TagState.{Daily, FrequencyType, Monthly}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class MessageProducer extends CalendarConverter with EnvLoader {
  private val kafkaConfig: Config = getConfig("kafka")

  private val producerConfig = kafkaConfig.getConfig("kafka.producer")
  private val publishTopic = kafkaConfig.getString("tag.publish-topic")
  private val tagName = kafkaConfig.getString("tag.name")

  val producer = KafkaProducer(
    KafkaProducer.Conf(
      new StringSerializer(),
      new StringSerializer()
    ).withConf(producerConfig)
  )

  def sendToFinishTopic(frequency: FrequencyType, dic: TagDictionary): Future[RecordMetadata] = {
    frequency match {
      case Daily =>
        val fMessage = TagFinishMessage(tagName, dic.tag_name, dic.update_frequency, None, Option(getDailyDate), dic.actorID, getCalendar.getTimeInMillis, is_success = true)
        val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
        producer.send(record)
      case Monthly =>
        val fMessage = TagFinishMessage(tagName, dic.tag_name, dic.update_frequency, Option(getLastMonth), None, dic.actorID, getCalendar.getTimeInMillis, is_success = true)
        val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
        producer.send(record)
    }
  }

}

object MessageProducer {
  private final val PRODUCER = new MessageProducer
  def getProducer: MessageProducer = PRODUCER
}
