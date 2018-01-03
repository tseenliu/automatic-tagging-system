package com.cathay.ddt.tagging.schema

/**
  * Created by Tse-En on 2017/12/19.
  */
case class TagMessage(kafkaTopic: String,
                      update_frequency: String,
                      value: String,
                      yyyymm: Option[String]=None,
                      yyyymmdd: Option[String]=None,
                      finish_time: Long) {
  import TagMessage._
  def getDefaultTM: Message = {
    TagMessage.SimpleTagMessage(update_frequency, value)
  }
}

object TagMessage {
  trait Message
  case class SimpleTagMessage(update_frequency: String, value: String) extends Message
}



