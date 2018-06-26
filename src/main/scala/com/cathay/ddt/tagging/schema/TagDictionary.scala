package com.cathay.ddt.tagging.schema

/**
  * Created by Tse-En on 2017/12/12.
  */

case class TagType(type_L1: String,
                   type_L2: String)

sealed trait Dictionary
case class TagDictionary(tag_id: String,
                         source_type: String,
                         source_item: String,
                         tag_type: List[TagType],
                         tag_name: String,
                         sql: String,
                         update_frequency: String,
                         started: Option[Int],
                         traced: Option[Int],
                         description: String,
                         create_time: String,
                         update_time: String,
                         disable_flag: Option[Boolean],
                         score_method: String,
                         attribute: String,
                         creator: String,
                         is_focus: Boolean,
                         system_name: String,
                         tickets: List[String]) extends Dictionary {
  val actorID: String = tag_id
}

case class DynamicTD(tag_id: Option[String],
                     source_type: Option[String],
                     source_item: Option[String],
                     tag_type: Option[List[TagType]],
                     tag_name: Option[String],
                     sql: Option[String],
                     update_frequency: Option[String],
                     started: Option[Int],
                     traced: Option[Int],
                     description: Option[String],
                     create_time: Option[String],
                     update_time: Option[String],
                     disable_flag: Option[Boolean],
                     score_method: Option[String],
                     attribute: Option[String],
                     creator: Option[String],
                     is_focus: Option[Boolean],
                     system_name: Option[String],
                     tickets: Option[List[String]]) extends Dictionary

case class QueryTD(source_type: Option[String],
                   source_item: Option[String],
                   tag_type: Option[List[TagType]],
                   tag_name: Option[String],
                   update_frequency: Option[String],
                   started: Option[Int],
                   traced: Option[Int],
                   score_method: Option[String],
                   attribute: Option[String],
                   system_name: Option[String]) extends Dictionary

case class ComposeTD(tag_id: String,
                     source_type: String,
                     source_item: String,
                     tag_type: List[TagType],
                     tag_name: String,
                     sql: String,
                     update_frequency: String,
                     started: Option[Int],
                     traced: Option[Int],
                     score_method: String,
                     attribute: String,
                     start_date: Option[String],
                     end_date: Option[String],
                     execute_date: String,
                     system_name: String) extends Dictionary {
  val actorID: String = tag_id
}
