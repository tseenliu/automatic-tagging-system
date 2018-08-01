package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.QuerySD
import spray.json.DefaultJsonProtocol

object QuerySDProtocol extends DefaultJsonProtocol {
  implicit val queryTDFormat = jsonFormat12(QuerySD)
}
