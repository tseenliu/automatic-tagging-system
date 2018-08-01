package com.cathay.ddt.tagging.protocal

import com.cathay.ddt.tagging.schema.QuerySD
import spray.json.DefaultJsonProtocol

object QueryTDProtocol extends DefaultJsonProtocol {
  implicit val queryTDFormat = jsonFormat12(QuerySD)
}
