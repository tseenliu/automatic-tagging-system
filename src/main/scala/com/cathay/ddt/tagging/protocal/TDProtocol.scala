package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.{TagDictionary, TagType}
import spray.json._

object TDProtocol extends DefaultJsonProtocol {
  import com.cathay.ddt.tagging.protocal.TagTypeProtocol._

  implicit object TdJsonFormat extends RootJsonFormat[TagDictionary] {
    def write(td: TagDictionary) = {
      val startedJV: JsValue = if (td.started.isDefined) JsNumber(td.started.get) else JsNull
      val tracedJV: JsValue = if (td.traced.isDefined) JsNumber(td.traced.get) else JsNull
      val disableFlagJV: JsValue = if (td.disable_flag.isDefined) JsBoolean(td.disable_flag.get) else JsNull
      JsObject(
        "tag_id" -> JsString(td.tag_id),
        "source_type" -> JsString(td.source_type),
        "source_item" -> JsString(td.source_item),
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
        "is_focus" -> JsBoolean(td.is_focus),
        "system_name" -> JsString(td.system_name)
      )
    }
    def read(value: JsValue): TagDictionary = {
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
        "description",
        "create_time",
        "update_time",
        "disable_flag",
        "score_method",
        "attribute",
        "creator",
        "is_focus",
        "system_name") match {
        case Seq(
        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNumber(started), JsNumber(traced), JsString(description), JsString(create_time), JsString(update_time), JsBoolean(disable_flag),
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus), JsString(system_name)) =>
          TagDictionary(
            tag_id, source_type, source_item, tagType, tag_name, sql, update_frequency, Some(started.toInt), Some(traced.toInt),
            description, create_time, update_time, Some(disable_flag), score_method, attribute, creator, is_focus, system_name)

        case Seq(
        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNumber(started), JsNumber(traced), JsString(description), JsString(create_time), JsString(update_time), JsNull,
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus), JsString(system_name)) =>
          TagDictionary(
            tag_id, source_type, source_item, tagType, tag_name, sql, update_frequency, Some(started.toInt), Some(traced.toInt),
            description, create_time, update_time, None, score_method, attribute, creator, is_focus, system_name)

        case Seq(
        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNull, JsNull, JsString(description), JsString(create_time), JsString(update_time), JsBoolean(disable_flag),
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus), JsString(system_name)) =>
          TagDictionary(
            tag_id, source_type, source_item, tagType, tag_name, sql, update_frequency, None, None,
            description, create_time, update_time, Some(disable_flag), score_method, attribute, creator, is_focus, system_name)

        case Seq(
        JsString(tag_id), JsString(source_type), JsString(source_item), JsString(tag_name), JsString(sql), JsString(update_frequency),
        JsNull, JsNull, JsString(description), JsString(create_time), JsString(update_time), JsNull,
        JsString(score_method), JsString(attribute), JsString(creator), JsBoolean(is_focus), JsString(system_name)) =>
          TagDictionary(
            tag_id, source_type, source_item, tagType, tag_name, sql, update_frequency, None, None,
            description, create_time, update_time, None, score_method, attribute, creator, is_focus, system_name)

        case _ => throw DeserializationException("Tag Dictionary Json formatted error.")
      }
    }
  }

}
