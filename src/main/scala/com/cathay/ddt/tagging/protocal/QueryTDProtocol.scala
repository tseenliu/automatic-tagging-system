package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.{QueryTD, TagType}
import spray.json.DefaultJsonProtocol

object QueryTDProtocol extends DefaultJsonProtocol {
  implicit val tagTypeFormat = jsonFormat2(TagType)
  implicit val queryTDFormat = jsonFormat10(QueryTD)
}
