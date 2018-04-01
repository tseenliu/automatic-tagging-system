package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.TagType
import spray.json._

object TagTypeProtocol extends DefaultJsonProtocol {
  implicit val tagTypeFormat = jsonFormat2(TagType)
}