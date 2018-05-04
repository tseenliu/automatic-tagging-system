package com.cathay.ddt.kafka

import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import com.cathay.ddt.tagging.schema.ComposeTD
import com.cathay.ddt.utils.CalendarConverter
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._
import TagJsonProtocol._
import com.cathay.ddt.ats.TagState.{Daily, FrequencyType, Monthly}
import org.slf4j.LoggerFactory

class MessageProducer extends CalendarConverter {

  val log = LoggerFactory.getLogger(this.getClass)

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

  def sendToFinishTopic( startTime: Long, frequency: FrequencyType, ctd: ComposeTD, messages: List[TM2Show]): Unit = {
    val finishTime = getCalendar.getTimeInMillis/1000
    val durationTime = finishTime - startTime
    frequency match {
      case Daily =>
        val fMessage =
          TagFinishMessage(
            tagName,
            ctd.actorID,
            ctd.tag_name,
            s"${ctd.actorID}_$finishTime",
            ctd.update_frequency,
            getCurrentDate,
            durationTime,
            finishTime,
            is_success = true,
            messages)
        val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
        producer.send(record)
      case Monthly =>
        val fMessage =
          TagFinishMessage(
            tagName,
            ctd.actorID,
            ctd.tag_name,
            s"${ctd.actorID}_$finishTime",
            ctd.update_frequency,
            getCurrentDate,
            durationTime,
            finishTime,
            is_success = true,
            messages)
        val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
        producer.send(record)
    }
    log.info(s"Tag($frequency) ID[${ctd.actorID}] is producing finish topic.")
  }

}

object MessageProducer {
  private final val PRODUCER = new MessageProducer
  def getProducer: MessageProducer = PRODUCER
}
