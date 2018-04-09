package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.{DynamicTD, TagType}
import spray.json.DefaultJsonProtocol

object DynamicTDProtocol extends DefaultJsonProtocol {
  implicit val tagTypeFormat = jsonFormat2(TagType)
  implicit val dynamicTDFormat = jsonFormat18(DynamicTD)
}
