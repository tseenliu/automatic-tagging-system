package com.cathay.ddt.tagging.schema

import reactivemongo.bson.BSONObjectID

/**
  * Created by Tse-En on 2017/12/12.
  */

case class CustomerDictionary(_id: BSONObjectID=BSONObjectID.generate,
                              channel_type: String,
                              segment_type: String,
                              segment_name: String,
                              sql: String,
                              update_frequency: String,
                              started: Option[Int],
                              traced: Option[Int],
                              description: String,
                              create_time: Long=CustomerDictionary.getCurrentTime,
                              update_time: Long=CustomerDictionary.getCurrentTime,
                              enable_flag: Boolean,
                              creator: String,
                              is_focus: Boolean) {
  val actorID: String = _id.stringify
}

object CustomerDictionary {
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