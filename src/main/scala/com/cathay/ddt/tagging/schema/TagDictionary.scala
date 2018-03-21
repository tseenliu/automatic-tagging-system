package com.cathay.ddt.tagging.schema

import spray.json._

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
case class TagType(type_L1: String,
                   type_L2: String)

case class TagDictionary(tag_id: String,
                         channel_type: String,
                         channel_item: String,
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
                         is_focus: Boolean) {
  val actorID: String = tag_id
}

object TagTypeProtocol extends DefaultJsonProtocol {
  implicit val tagTypeFormat = jsonFormat2(TagType)
}

object TagDictionaryProtocol extends DefaultJsonProtocol {
  import com.cathay.ddt.tagging.schema.TagTypeProtocol._


  implicit object ColorJsonFormat extends RootJsonFormat[TagDictionary] {
    def write(td: TagDictionary) = {
      val startedJV: JsValue = if (td.started.isDefined) JsNumber(td.started.get) else JsNull
      val tracedJV: JsValue = if (td.traced.isDefined) JsNumber(td.traced.get) else JsNull
      val disableFlagJV: JsValue = if (td.disable_flag.isDefined) JsBoolean(td.disable_flag.get) else JsNull
      JsObject(
        "tag_id" -> JsString(td.tag_id),
        "channel_type" -> JsString(td.channel_type),
        "channel_item" -> JsString(td.channel_item),
        "tag_type" ->  td.tag_type.toJson,
        "tag_name" -> JsString(td.tag_name),
        "sql" -> JsString(td.sql),
        "update_frequency" -> JsString(td.update_frequency),
        "started" -> startedJV,
        "traced" -> tracedJV,
        "description" -> JsString(td.description),
        "create_time" -> JsString(td.create_time),
        "update_time" -> JsString(td.update_time),
        "disable_flag" -> disableFlagJV,
        "score_method" -> JsString(td.score_method),
        "attribute" -> JsString(td.attribute),
        "creator" -> JsString(td.creator),
        "is_focus" -> JsBoolean(td.is_focus)
      )
    }
    def read(value: JsValue): TagDictionary = {
      val jso = value.asJsObject
      val tagType = jso.fields("tag_type").convertTo[List[TagType]]

      value.asJsObject.getFields(
        "tag_id",
        "channel_type",
        "channel_item",
        "tag_name",
        "sql",
        "update_frequency",
        "started",
        "traced",
        "description",
        "create_time",
        "update_time",
        "disable_flag",
        "score_method",
        "attribute",
        "creator",
        "is_focus") match {
        case Seq(
        JsString(tag_id), JsString(channel_type), JsString(channel_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNumber(started), JsNumber(traced), JsString(description), JsString(create_time), JsString(update_time), JsBoolean(disable_flag),
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus)) =>
          TagDictionary(
            tag_id, channel_type,channel_item, tagType, tag_name, sql, update_frequency, Some(started.toInt), Some(traced.toInt),
            description, create_time, update_time, Some(disable_flag), score_method, attribute, creator, is_focus)

        case Seq(
        JsString(tag_id), JsString(channel_type), JsString(channel_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNumber(started), JsNumber(traced), JsString(description), JsString(create_time), JsString(update_time), JsNull,
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus)) =>
          TagDictionary(
            tag_id, channel_type,channel_item, tagType, tag_name, sql, update_frequency, Some(started.toInt), Some(traced.toInt),
            description, create_time, update_time, None, score_method, attribute, creator, is_focus)

        case Seq(
        JsString(tag_id), JsString(channel_type), JsString(channel_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNull, JsNull, JsString(description), JsString(create_time), JsString(update_time), JsBoolean(disable_flag),
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus)) =>
          TagDictionary(
            tag_id, channel_type,channel_item, tagType, tag_name, sql, update_frequency, None, None,
            description, create_time, update_time, Some(disable_flag), score_method, attribute, creator, is_focus)

        case Seq(
        JsString(tag_id), JsString(channel_type), JsString(channel_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNull, JsNull, JsString(description), JsString(create_time), JsString(update_time), JsNull,
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus)) =>
          TagDictionary(
            tag_id, channel_type,channel_item, tagType, tag_name, sql, update_frequency, None, None,
            description, create_time, update_time, None, score_method, attribute, creator, is_focus)
        case _ => throw DeserializationException("Tag Dictionary Json formatted error.")
      }
    }
  }

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