package com.cathay.ddt.kafka

import com.cathay.ddt.tagging.schema.TagMessage.SimpleTagMessage
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

case class StartMessage(hippo_name: String,
                        job_name: String,
                        job_id: String,
                        need_msgs: List[SimpleTagMessage],
                        start_time: Long)

case class TM2Show(kafkaTopic: Option[String],
                   update_frequency: String,
                   value: String,
                   partition_fields: Option[String]=Some("yyyymm"),
                   partition_values: Option[String],
                   finish_time: Option[Long])

case class FinishMessage(hippo_name: String, //batchetl.tagging
                         tag_id: String, //id
                         job_name: String, //超市購物
                         job_id: String, //tag_id_timestamp(10)
                         update_frequency: String,
                         execute_time: String, // execute time
                         duration_time: Long,
                         finish_time: Long,
                         is_success: Boolean,
                         receive_msgs: List[TM2Show])

case class SegmentFinishMessage(hippo_name: String, //batchetl.tagging
                         job_name: String, //超市購物
                         job_id: String, //tag_id_timestamp(10)
                         update_frequency: String,
                         execute_time: String, // execute time
                         duration_time: Long,
                         finish_time: Long,
                         is_success: Boolean,
                         receive_msgs: List[TM2Show])

object TagJsonProtocol extends DefaultJsonProtocol {
  implicit val frontierFormat: RootJsonFormat[FrontierMessage] = jsonFormat6(FrontierMessage)
  implicit val tagMessageFormat: RootJsonFormat[TM2Show] = jsonFormat6(TM2Show)
  implicit val tagFinishFormat: RootJsonFormat[FinishMessage] = jsonFormat10(FinishMessage)
  implicit val segmentFinishFormat: RootJsonFormat[SegmentFinishMessage] = jsonFormat9(SegmentFinishMessage)
  implicit val stmFormat: RootJsonFormat[SimpleTagMessage] = jsonFormat3(SimpleTagMessage)
  implicit val startMessageFormat: RootJsonFormat[StartMessage] = jsonFormat5(StartMessage)
}
