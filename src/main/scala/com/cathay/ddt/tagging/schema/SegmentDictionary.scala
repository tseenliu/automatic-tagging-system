package com.cathay.ddt.tagging.schema

/**
  * Created by Tse-En on 2017/12/12.
  */

sealed trait Dictionary
case class SegmentDictionary(segment_id: String,
                             segment_type: String,
                             segment_name: String,
                             sql: String,
                             update_frequency: String,
                             detail: String,
                             description: String,
                             create_time: String,
                             update_time: String,
                             disable_flag: Option[Boolean],
                             creator: String,
                             is_focus: Boolean,
                             tickets: List[String]) extends Dictionary {
  val actorID: String = segment_id
}

case class DynamicSD(segment_id: Option[String],
                     segment_type: Option[String],
                     segment_name: Option[String],
                     sql: Option[String],
                     update_frequency: Option[String],
                     detail: Option[String],
                     description: Option[String],
                     create_time: Option[String],
                     update_time: Option[String],
                     disable_flag: Option[Boolean],
                     creator: Option[String],
                     is_focus: Option[Boolean],
                     tickets: Option[List[String]]) extends Dictionary

case class QuerySD(segment_id: Option[String],
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
                   tickets: Option[List[String]]) extends Dictionary

case class ComposeSD(segment_id: String,
                     segment_type: String,
                     segment_name: String,
                     sql: String,
                     update_frequency: String,
                     execute_date: String) extends Dictionary {
  val actorID: String = segment_id
}
