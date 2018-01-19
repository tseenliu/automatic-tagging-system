package com.cathay.ddt.kafka

import spray.json.DefaultJsonProtocol

/**
  * Created by Tse-En on 2017/12/23.
  */

case class FrontierMessage(table: String,
                           partition_fields: Array[String],
                           partition_values: Array[String],
                           db: String,
                           method: String,
                           exec_date: Long) {
//    override def toString: String = {
//      s"$table, $partition_fields}, $partition_values, $db, $method, $exec_date"
//    }
}

case class TagFinishMessage(hippo_name: String,  //batchetl.tagging
                            job_name: String,    //超市購物
                            update_frequency: String,
                            yyyymmdd: String,
                            tag_id: String,     //id
                            finish_time: Long,
                            is_success: Boolean)

object TagJsonProtocol extends DefaultJsonProtocol {
  implicit val frontierFormat = jsonFormat6(FrontierMessage)
  implicit val tagFinishFormat = jsonFormat7(TagFinishMessage)
}
