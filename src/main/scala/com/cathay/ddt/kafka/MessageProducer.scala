package com.cathay.ddt.kafka

import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import com.cathay.ddt.tagging.schema.ComposeTD
import com.cathay.ddt.utils.CalendarConverter
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._
import TagJsonProtocol._
import com.cathay.ddt.tagging.schema.TagMessage.SimpleTagMessage
import org.slf4j.LoggerFactory

class MessageProducer extends CalendarConverter {

  val log = LoggerFactory.getLogger(this.getClass)

  private val kafkaConfig: Config = getConfig("kafka")
  private val producerConfig = kafkaConfig.getConfig("kafka.producer")
  private val startTopic = kafkaConfig.getString("tag.startmsg-topic")
  private val publishTopic = kafkaConfig.getString("tag.finishmsg-topic")
  private val tagName = kafkaConfig.getString("tag.name")

  val producer = KafkaProducer(
    KafkaProducer.Conf(
      new StringSerializer(),
      new StringSerializer()
    ).withConf(producerConfig)
  )

  def sendToStart(startTime: Long, ctd: ComposeTD, messages: List[SimpleTagMessage]): Unit = {
    val sMessage =
      StartMessage(
        tagName,
        ctd.tag_name,
        s"${ctd.actorID}_$startTime",
        messages,
        startTime)
    val record = KafkaProducerRecord(startTopic, Some("tagKey"), s"${sMessage.toJson.prettyPrint}")
    producer.send(record)
    log.info(s"Tag(${ctd.update_frequency}) ID[${ctd.actorID}] is producing started topic.")
  }

  def sendToFinishTopic(startTime: Long, ctd: ComposeTD, messages: List[TM2Show], is_success: Boolean): Unit = {
    val finishTime = getCalendar.getTimeInMillis/1000
    val durationTime = finishTime - startTime
    val fMessage =
      FinishMessage(
        tagName,
        ctd.actorID,
        ctd.tag_name,
        s"${ctd.actorID}_$startTime",
        ctd.update_frequency,
        getCurrentDate,
        durationTime,
        finishTime,
        is_success,
        messages)
    val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
    producer.send(record)
    log.info(s"Tag(${ctd.update_frequency}) ID[${ctd.actorID}] is producing finished topic.")
//    frequency match {
//      case Daily =>
//        val fMessage =
//          TagFinishMessage(
//            tagName,
//            ctd.actorID,
//            ctd.tag_name,
//            s"${ctd.actorID}_$finishTime",
//            ctd.update_frequency,
//            getCurrentDate,
//            durationTime,
//            finishTime,
//            is_success = true,
//            messages)
//        val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
//        producer.send(record)
//      case Monthly =>
//        val fMessage =
//          TagFinishMessage(
//            tagName,
//            ctd.actorID,
//            ctd.tag_name,
//            s"${ctd.actorID}_$finishTime",
//            ctd.update_frequency,
//            getCurrentDate,
//            durationTime,
//            finishTime,
//            is_success = true,
//            messages)
//        val record = KafkaProducerRecord(publishTopic, Some("tagKey"), s"${fMessage.toJson.prettyPrint}")
//        producer.send(record)
//    }
  }

}

object MessageProducer {
  private final val PRODUCER = new MessageProducer
  def getProducer: MessageProducer = PRODUCER
}
