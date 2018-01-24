package com.cathay.ddt.tagging.schema

import reactivemongo.bson.BSONObjectID

/**
  * Created by Tse-En on 2017/12/12.
  */

case class TagDictionary(_id: BSONObjectID=BSONObjectID.generate,
                         channel_type: String,
                         channel_name: String,
                         tag_type: String,
                         tag_name: String,
                         sql: String,
                         update_frequency: String,
                         started: Option[Int],
                         traced: Option[Int],
                         description: String,
                         create_time: Long=TagDictionary.getCurrentTime,
                         update_time: Long=TagDictionary.getCurrentTime,
                         enable_flag: Boolean,
                         score_option: String,
                         attribute: String,
                         creator: String,
                         is_focus: Boolean) {
  val actorID: String = _id.stringify
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