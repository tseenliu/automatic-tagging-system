package com.cathay.ddt.tagging.schema

import spray.json._

/**
  * Created by Tse-En on 2017/12/12.
  */

//case class TagDictionary(tag_id: String,
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

//case class TagDictionary(tag_id: String,
//                         source_type: String,
//                         source_item: String,
//                         tag_type: String,
//                         tag_name: String,
//                         sql: String,
//                         update_frequency: String,
//                         started: Option[Int],
//                         traced: Option[Int],
//                         score_method: String,
//                         attribute: String,
//                         start_date: String,
//                         end_date: String,
//                         execute_date: String,
//                         system_name: String) {
//  val actorID: String = tag_id
//}


// ImplicitExtensionMethod
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
                         system_name: String) extends Dictionary {
  val actorID: String = tag_id
}

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

object TagTypeProtocol extends DefaultJsonProtocol {
  implicit val tagTypeFormat = jsonFormat2(TagType)
}

object ComposeTDProtocol extends DefaultJsonProtocol {
  import com.cathay.ddt.tagging.schema.TagTypeProtocol._


  implicit object ComposeTdJsonFormat extends RootJsonFormat[ComposeTD] {
    def write(ctd: ComposeTD) = {
      val startedJV: JsValue = if (ctd.started.isDefined) JsNumber(ctd.started.get) else JsNull
      val tracedJV: JsValue = if (ctd.traced.isDefined) JsNumber(ctd.traced.get) else JsNull
      val startDateJV: JsValue = if (ctd.start_date.isDefined) JsString(ctd.start_date.get) else JsNull
      val endDateJV: JsValue = if (ctd.end_date.isDefined) JsString(ctd.end_date.get) else JsNull
//      val disableFlagJV: JsValue = if (td.disable_flag.isDefined) JsBoolean(td.disable_flag.get) else JsNull
      JsObject(
        "tag_id" -> JsString(ctd.tag_id),
        "source_type" -> JsString(ctd.source_type),
        "source_item" -> JsString(ctd.source_item),
        "tag_type" ->  ctd.tag_type.toJson,
        "tag_name" -> JsString(ctd.tag_name),
        "sql" -> JsString(ctd.sql),
        "update_frequency" -> JsString(ctd.update_frequency),
        "started" -> startedJV,
        "traced" -> tracedJV,
//        "description" -> JsString(td.description),
//        "create_time" -> JsString(td.create_time),
//        "update_time" -> JsString(td.update_time),
//        "disable_flag" -> disableFlagJV,
        "score_method" -> JsString(ctd.score_method),
        "attribute" -> JsString(ctd.attribute),
        "start_date" -> startDateJV,
        "end_date" -> endDateJV,
        "execute_date" -> JsString(ctd.execute_date),
        "system_name" -> JsString(ctd.system_name)
//        "creator" -> JsString(td.creator),
//        "is_focus" -> JsBoolean(td.is_focus)
      )
    }
    def read(value: JsValue): ComposeTD = {
      val jso = value.asJsObject
      val tagType = jso.fields("tag_type").convertTo[List[TagType]]

      value.asJsObject.getFields(
        "tag_id",
        "source_type",
        "source_item",
        "tag_name",
        "sql",
        "update_frequency",
        "started",
        "traced",
        "score_method",
        "attribute",
        "start_date",
        "end_date",
        "execute_date",
        "system_name") match {
        case Seq(
        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNumber(started), JsNumber(traced), JsString(score_method), JsString(attribute), JsString(start_date), JsString(end_date),
        JsString(execute_date), JsString(system_name)) =>
          ComposeTD(
            tag_id, source_type,source_item, tagType, tag_name, sql, update_frequency, Some(started.toInt), Some(traced.toInt),
            score_method, attribute, Some(start_date), Some(end_date), execute_date, system_name)

        case Seq(
        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNull, JsNull, JsString(score_method), JsString(attribute), JsNull, JsNull, JsString(execute_date), JsString(system_name)) =>
          ComposeTD(
            tag_id, source_type,source_item, tagType, tag_name, sql, update_frequency, None, None,
            score_method, attribute, None, None, execute_date, system_name)

        case _ => throw DeserializationException("Tag Dictionary Json formatted error.")
        //        case Seq(
        //        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        //        JsNumber(started), JsNumber(traced), JsString(description), JsString(create_time), JsString(update_time), JsNull,
        //        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus)) =>
        //          TagDictionary(
        //            tag_id, source_type,source_item, tagType, tag_name, sql, update_frequency, Some(started.toInt), Some(traced.toInt),
        //            description, create_time, update_time, None, score_method, attribute, creator, is_focus)
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