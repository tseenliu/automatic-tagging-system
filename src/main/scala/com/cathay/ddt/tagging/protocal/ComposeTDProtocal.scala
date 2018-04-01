package com.cathay.ddt.tagging.protocal

import spray.json._
import com.cathay.ddt.tagging.schema.{ComposeTD, TagType}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsNull, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

object ComposeTDProtocal extends DefaultJsonProtocol {
  import com.cathay.ddt.tagging.protocal.TagTypeProtocol._


  implicit object ComposeTdJsonFormat extends RootJsonFormat[ComposeTD] {
    def write(ctd: ComposeTD) = {
      val startedJV: JsValue = if (ctd.started.isDefined) JsNumber(ctd.started.get) else JsNull
      val tracedJV: JsValue = if (ctd.traced.isDefined) JsNumber(ctd.traced.get) else JsNull
      val startDateJV: JsValue = if (ctd.start_date.isDefined) JsString(ctd.start_date.get) else JsNull
      val endDateJV: JsValue = if (ctd.end_date.isDefined) JsString(ctd.end_date.get) else JsNull
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
        "score_method" -> JsString(ctd.score_method),
        "attribute" -> JsString(ctd.attribute),
        "start_date" -> startDateJV,
        "end_date" -> endDateJV,
        "execute_date" -> JsString(ctd.execute_date),
        "system_name" -> JsString(ctd.system_name)
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
      }
    }
  }

}