package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.DynamicSD
import spray.json.DefaultJsonProtocol

object DynamicTDProtocol extends DefaultJsonProtocol {
  implicit val dynamicTDFormat = jsonFormat12(DynamicSD)
}
