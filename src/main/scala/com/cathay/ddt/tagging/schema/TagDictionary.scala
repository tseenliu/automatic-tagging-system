package com.cathay.ddt.tagging.schema

import reactivemongo.bson.BSONObjectID

/**
  * Created by Tse-En on 2017/12/12.
  */

//case class TagDictionary(_id: BSONObjectID=BSONObjectID.generate,
//                         tag_id: String,
//                         channel_type: String,
//                         channel_item: String,
//                         tag_type: String,
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
//                         is_focus: Boolean) {
//  val actorID: String = tag_id
//}


// ImplicitExtensionMethod
case class Type(type_L1: String,
                type_L2: String)

case class TagDictionary(tag_id: String,
                         channel_type: String,
                         channel_item: String,
                         tag_type: List[Type],
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
                         is_focus: Boolean) {
  val actorID: String = tag_id
}

object TagDictionary {
  def getCurrentTime: Long = System.currentTimeMillis()

  // Response
  sealed trait Response
  object Response {
    case object HippoExists extends Response
    case object HippoNotFound extends Response
    case object EntryCmdSuccess extends Response
    case object StateCmdSuccess extends Response
    case class StateCmdException(reason: String) extends Response
    case class StateCmdUnhandled(currentState: String) extends Response
  }
}