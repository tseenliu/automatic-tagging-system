package com.cathay.ddt.kafka

import spray.json.DefaultJsonProtocol

/**
  * Created by Tse-En on 2017/12/23.
  */

case class FrontierMessage(
                            table: String,
                            partition_fields: Array[String],
                            partition_values: Array[String],
                            db: String,
                            method: String,
                            exec_date: Long) {
//    override def toString: String = {
//      s"$table, $partition_fields}, $partition_values, $db, $method, $exec_date"
//    }
}

case class TagFinishMessage(
                              hippo_name: String,
                              job_name: String,
                              update_frequency: String,
                              channel_type: String,
                              tag_id: String,
                              finish_time: Long,
                              is_success: Boolean
                            )

// Testing
case class TagMessageTest(kafkaTopic: String,
                      update_frequency: String,
                      value: String,
                      yyyymm: Option[String]=None,
                      yyyymmdd: Option[String]=None,
                      finish_time: Long)

object TagJsonProtocol extends DefaultJsonProtocol {
  implicit val frontierFormat = jsonFormat6(FrontierMessage)
  implicit val tagFinishFormat = jsonFormat7(TagFinishMessage)
  implicit val tagFormat = jsonFormat6(TagMessageTest)
}
