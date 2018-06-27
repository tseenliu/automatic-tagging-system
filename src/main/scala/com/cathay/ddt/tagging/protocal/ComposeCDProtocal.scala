package com.cathay.ddt.tagging.protocal

import spray.json._
import com.cathay.ddt.tagging.schema.ComposeCD
import spray.json.{DefaultJsonProtocol, DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

object ComposeCDProtocal extends DefaultJsonProtocol {

  implicit object ComposeTdJsonFormat extends RootJsonFormat[ComposeCD] {
    def write(ctd: ComposeCD) = {
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
        "creator" -> JsString(ctd.creator),
        "is_focus" -> JsBoolean(ctd.is_focus),
        "tickets" -> ctd.tickets.toJson,
        "execute_date" -> JsString(ctd.execute_date)
      )
    }
    def read(value: JsValue): ComposeCD = {
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
        "creator",
        "is_focus",
        "execute_date") match {
        case Seq(
        JsString(segment_id), JsString(segment_type), JsString(segment_name), JsString(sql), JsString(update_frequency), JsString(detail),
        JsString(description), JsString(create_time), JsString(update_time), JsString(creator), JsBoolean(is_focus), JsString(execute_date)) =>
          ComposeCD(
            segment_id, segment_type, segment_name, sql, update_frequency, detail, description,
            create_time, update_time, creator, is_focus, tickets, execute_date)

        case _ => throw DeserializationException("Tag Dictionary Json formatted error.")
      }
    }
  }

}