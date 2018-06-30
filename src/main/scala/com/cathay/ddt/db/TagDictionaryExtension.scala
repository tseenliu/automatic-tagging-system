package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.TagDictionary
import reactivemongo.bson.Macros

class TagDictionaryExtension {
  implicit val reader = Macros.reader[TagDictionary]
    implicit val writer = Macros.writer[TagDictionary]
}
