package com.cathay.ddt.tagging.schema

/**
  * Created by Tse-En on 2017/12/19.
  */
case class RequiredMessage(kafkaTopic: String, update_frequency: String, value: String, partitioned: String)
case class TagMessage(kafkaTopic: String,
                      update_frequency: String,
                      value: String,
                      partition_fields: String="yyyymm",
                      partition_values: String,
                      finish_time: Long) {
  import TagMessage._
  def getDefaultTM: Message = {
    TagMessage.SimpleTagMessage(update_frequency, value, partition_fields)
  }
}

object TagMessage {
  trait Message
  case class SimpleTagMessage(update_frequency: String, value: String, partition_fields: String="yyyymm") extends Message
}



