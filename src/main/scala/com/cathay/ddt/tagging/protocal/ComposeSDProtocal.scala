package com.cathay.ddt.tagging.protocal

import spray.json._
import com.cathay.ddt.tagging.schema.ComposeSD
import spray.json.{DefaultJsonProtocol, DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

object ComposeSDProtocal extends DefaultJsonProtocol {

  implicit object ComposeSDJsonFormat extends RootJsonFormat[ComposeSD] {
    def write(ctd: ComposeSD) = {
      JsObject(
        "segment_id" -> JsString(ctd.segment_id),
        "segment_type" -> JsString(ctd.segment_type),
        "segment_name" -> JsString(ctd.segment_name),
        "sql" -> JsString(ctd.sql),
        "update_frequency" -> JsString(ctd.update_frequency),
        "execute_date" -> JsString(ctd.execute_date)
      )
    }
    def read(value: JsValue): ComposeSD = {
      val jso = value.asJsObject

      value.asJsObject.getFields(
        "segment_id",
        "segment_type",
        "segment_name",
        "sql",
        "update_frequency",
        "execute_date") match {
        case Seq(
        JsString(segment_id), JsString(segment_type), JsString(segment_name),
        JsString(sql), JsString(update_frequency), JsString(execute_date)) =>
          ComposeSD(segment_id, segment_type, segment_name, sql, update_frequency, execute_date)

        case _ => throw DeserializationException("Tag Dictionary Json formatted error.")
      }
    }
  }

}