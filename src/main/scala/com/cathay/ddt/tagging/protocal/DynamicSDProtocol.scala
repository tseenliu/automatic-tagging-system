package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.DynamicSD
import spray.json.DefaultJsonProtocol

object DynamicSDProtocol extends DefaultJsonProtocol {
  implicit val dynamicTDFormat = jsonFormat13(DynamicSD)
}
