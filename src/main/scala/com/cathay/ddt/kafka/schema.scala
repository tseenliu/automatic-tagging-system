package com.cathay.ddt.kafka

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
  * Created by Tse-En on 2017/12/23.
  */

case class FrontierMessage(table: String,
                           partition_fields: Array[String],
                           partition_values: Array[String],
                           db: String,
                           method: String,
                           exec_date: Long)

case class TM2Show(kafkaTopic: Option[String],
                   update_frequency: String,
                   value: String,
                   partition_fields: Option[String]=Some("yyyymm"),
                   partition_values: Option[String],
                   finish_time: Option[Long])

case class TagFinishMessage(hippo_name: String,  //batchetl.tagging
                            tag_id: String,     //id
                            job_name: String,    //超市購物
                            job_id: String,     //tag_id_timestamp(10)
                            update_frequency: String,
                            execute_time: String,  // execute time
                            duration_time: Long,
                            finish_time: Long,
                            is_success: Boolean,
                            receive_msgs: List[TM2Show])

object TagJsonProtocol extends DefaultJsonProtocol {
  implicit val frontierFormat: RootJsonFormat[FrontierMessage] = jsonFormat6(FrontierMessage)
  implicit val tagMessageFormat: RootJsonFormat[TM2Show] = jsonFormat6(TM2Show)
  implicit val tagFinishFormat: RootJsonFormat[TagFinishMessage] = jsonFormat10(TagFinishMessage)
}
