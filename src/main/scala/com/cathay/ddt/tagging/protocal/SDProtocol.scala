package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.SegmentDictionary
import spray.json._

object SDProtocol extends DefaultJsonProtocol {

  implicit object TdJsonFormat extends RootJsonFormat[SegmentDictionary] {
    def write(ctd: SegmentDictionary) = {
      val disableFlagJV = if (ctd.disable_flag.isDefined) JsBoolean(ctd.disable_flag.get) else JsNull
      JsObject(
        "segment_id" -> JsString(ctd.segment_id),
        "segment_type" -> JsString(ctd.segment_type),
        "segment_name" -> JsString(ctd.segment_name),
        "sql" -> JsString(ctd.sql),
        "update_frequency" -> JsString(ctd.update_frequency),
        "detail" -> JsString(ctd.detail),
        "description" -> JsString(ctd.description),
        "create_time" -> JsString(ctd.create_time),
        "update_time" -> JsString(ctd.update_time),
        "disable_flag" -> disableFlagJV,
        "creator" -> JsString(ctd.creator),
        "is_focus" -> JsBoolean(ctd.is_focus),
        "tickets" -> ctd.tickets.toJson
      )
    }
    def read(value: JsValue): SegmentDictionary = {
      val jso = value.asJsObject
      val tickets = jso.fields("tickets").convertTo[List[String]]

      value.asJsObject.getFields(
        "segment_id",
        "segment_type",
        "segment_name",
        "sql",
        "update_frequency",
        "detail",
        "description",
        "create_time",
        "update_time",
        "disable_flag",
        "creator",
        "is_focus") match {
        case Seq(
        JsString(segment_id), JsString(segment_type), JsString(segment_name), JsString(sql), JsString(update_frequency), JsString(detail),
        JsString(description), JsString(create_time), JsString(update_time), JsString(creator), JsBoolean(is_focus)) =>
          SegmentDictionary(
            segment_id, segment_type, segment_name, sql, update_frequency, detail, description,
            create_time, update_time, creator, is_focus, tickets)

        case Seq(
        JsString(segment_id), JsString(segment_type), JsString(segment_name), JsString(sql), JsString(update_frequency), JsString(detail),
        JsString(description), JsString(create_time), JsString(update_time), JsString(creator), JsBoolean(is_focus)) =>
          SegmentDictionary(
            segment_id, segment_type, segment_name, sql, update_frequency, detail, description,
            create_time, update_time, creator, is_focus, tickets)

        case _ => throw DeserializationException("Segment Dictionary Json formatted error.")
      }
    }
  }

}
