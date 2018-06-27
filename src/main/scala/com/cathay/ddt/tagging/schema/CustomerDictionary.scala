package com.cathay.ddt.tagging.schema

/**
  * Created by Tse-En on 2017/12/12.
  */

sealed trait Dictionary
case class CustomerDictionary(segment_id: String,
                              segment_type: String,
                              segment_name: String,
                              sql: String,
                              update_frequency: String,
                              detail: String,
                              description: String,
                              create_time: String,
                              update_time: String,
                              creator: String,
                              is_focus: Boolean,
                              tickets: List[String]) extends Dictionary {
  val actorID: String = segment_id
}

//case class TagDictionary(tag_id: String,
//                         source_type: String,
//                         source_item: String,
//                         tag_type: List[TagType],
//                         tag_name: String,
//                         sql: String,
//                         update_frequency: String,
//                         started: Option[Int],
//                         traced: Option[Int],
//                         description: String,
//                         create_time: String,
//                         update_time: String,
//                         disable_flag: Option[Boolean],
//                         score_method: String,
//                         attribute: String,
//                         creator: String,
//                         is_focus: Boolean,
//                         system_name: String,
//                         tickets: List[String]) extends Dictionary {
//  val actorID: String = tag_id
//}

case class DynamicCD(segment_id: Option[String],
                     segment_type: Option[String],
                     segment_name: Option[String],
                     sql: Option[String],
                     update_frequency: Option[String],
                     detail: Option[String],
                     description: Option[String],
                     create_time: Option[String],
                     update_time: Option[String],
                     creator: Option[String],
                     is_focus: Option[Boolean],
                     tickets: List[Option[String]]) extends Dictionary

case class QueryCD(segment_id: Option[String],
                   segment_type: Option[String],
                   segment_name: Option[String],
                   sql: Option[String],
                   update_frequency: Option[String],
                   detail: Option[String],
                   description: Option[String],
                   create_time: Option[String],
                   update_time: Option[String],
                   creator: Option[String],
                   is_focus: Option[Boolean],
                   tickets: List[Option[String]]) extends Dictionary

case class ComposeCD(segment_id: String,
                     segment_type: String,
                     segment_name: String,
                     sql: String,
                     update_frequency: String,
                     detail: String,
                     description: String,
                     create_time: String,
                     update_time: String,
                     creator: String,
                     is_focus: Boolean,
                     tickets: List[String],
                     execute_date: String) extends Dictionary {
  val actorID: String = segment_id
}
