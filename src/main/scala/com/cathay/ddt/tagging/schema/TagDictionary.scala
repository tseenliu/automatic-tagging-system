package com.cathay.ddt.tagging.schema

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
                         tickets: List[String]) {
  val actorID: String = tag_id
}

case class TagType(type_L1: String,
                   type_L2: String)
