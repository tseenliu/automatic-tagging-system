package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.DynamicCD
import spray.json.DefaultJsonProtocol

object DynamicTDProtocol extends DefaultJsonProtocol {
  implicit val dynamicTDFormat = jsonFormat12(DynamicCD)
}
