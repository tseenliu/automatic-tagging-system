package com.cathay.ddt.tagging.schema

import com.cathay.ddt.kafka.TM2Show

/**
  * Created by Tse-En on 2017/12/19.
  */

case class TagMessage(kafkaTopic: Option[String],
                      update_frequency: String,
                      value: String,
                      partition_fields: Option[String]=None,
                      partition_values: Option[String],
                      finish_time: Option[Long]) {
  import TagMessage.SimpleTagMessage
  def getDefaultTM: SimpleTagMessage = {
    SimpleTagMessage(update_frequency, value, partition_fields)
  }
}

object TagMessage {
  trait Message
  case class SimpleTagMessage(update_frequency: String, value: String, partition_fields: Option[String]=None) extends Message

  implicit def convertTM(stm: SimpleTagMessage): TagMessage = {
    TagMessage(
      None,
      stm.update_frequency,
      stm.value,
      stm.partition_fields,
      None,
      None
    )
  }

  implicit def convertSTM(tm: TagMessage): SimpleTagMessage = {
    SimpleTagMessage(
      tm.update_frequency,
      tm.value,
      tm.partition_fields
    )
  }

  implicit def convertTM2(tm: TagMessage): TM2Show = {
    TM2Show(
      tm.kafkaTopic,
      tm.update_frequency,
      tm.value,
      tm.partition_fields,
      tm.partition_values,
      tm.finish_time
    )
  }
}



