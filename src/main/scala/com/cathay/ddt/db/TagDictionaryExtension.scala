package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.{TagDictionary, TagType}
import reactivemongo.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter, Macros}

class TagDictionaryExtension {

//  implicit val reader = Macros.reader[TagDictionary]
//  implicit val writer = Macros.writer[TagDictionary]

  implicit object TagTypeWriter extends BSONDocumentWriter[TagType] {
    def write(album: TagType): BSONDocument = BSONDocument(
      "type_L1" -> album.type_L1,
      "type_L2" -> album.type_L2)
  }

  implicit object TagTypeReader extends BSONDocumentReader[TagType] {
    def read(doc: BSONDocument): TagType = {
      TagType(
        doc.getAs[String]("type_L1").get,
        doc.getAs[String]("type_L2").get)
    }
  }


  implicit object TDWriter extends BSONDocumentWriter[TagDictionary] {
    def write(td: TagDictionary): BSONDocument = BSONDocument(
      "tag_id" -> td.tag_id,
      "source_type" -> td.source_type,
      "source_item" -> td.source_item,
      "tag_type" -> td.tag_type,
      "tag_name" -> td.tag_name,
      "sql" -> td.sql,
      "update_frequency" -> td.update_frequency,
      "started" -> td.started,
      "traced" -> td.traced,
      "description" -> td.description,
      "update_time"  -> td.update_time,
      "create_time" -> td.create_time,
      "disable_flag" -> td.disable_flag,
      "score_method" -> td.score_method,
      "attribute" -> td.attribute,
      "creator" -> td.creator,
      "is_focus" -> td.is_focus,
      "system_name" -> td.system_name,
      "tickets" -> td.tickets)
  }

  implicit object TDReader extends BSONDocumentReader[TagDictionary] {
    def read(doc: BSONDocument): TagDictionary = TagDictionary(
      doc.getAs[String]("tag_id").get,
      doc.getAs[String]("source_type").get,
      doc.getAs[String]("source_item").get,
      doc.getAs[List[TagType]]("tag_type").toList.flatten,
      doc.getAs[String]("tag_name").get,
      doc.getAs[String]("sql").get,
      doc.getAs[String]("update_frequency").get,
      doc.getAs[Int]("started"),
      doc.getAs[Int]("traced"),
      doc.getAs[String]("description").get,
      doc.getAs[String]("update_time").get,
      doc.getAs[String]("create_time").get,
      doc.getAs[Boolean]("disable_flag"),
      doc.getAs[String]("score_method").get,
      doc.getAs[String]("attribute").get,
      doc.getAs[String]("creator").get,
      doc.getAs[Boolean]("is_focus").get,
      doc.getAs[String]("system_name").get,
      doc.getAs[List[String]]("tickets").toList.flatten)
  }

}
