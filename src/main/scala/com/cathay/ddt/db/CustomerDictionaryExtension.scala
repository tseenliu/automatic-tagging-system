package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.CustomerDictionary
import reactivemongo.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter, Macros}

class CustomerDictionaryExtension {
//  implicit val reader = Macros.reader[TagDictionary]
//  implicit val writer = Macros.writer[TagDictionary]

  implicit object TDWriter extends BSONDocumentWriter[CustomerDictionary] {
    def write(ctd: CustomerDictionary): BSONDocument = BSONDocument(
      "segment_id" -> ctd.segment_id,
      "segment_type" -> ctd.segment_type,
      "segment_name" -> ctd.segment_name,
      "sql" -> ctd.sql,
      "update_frequency" -> ctd.update_frequency,
      "detail" -> ctd.detail,
      "description" -> ctd.description,
      "create_time" -> ctd.create_time,
      "update_time" -> ctd.update_time,
      "creator" -> ctd.creator,
      "is_focus" -> ctd.is_focus,
      "tickets" -> ctd.tickets)
  }

  implicit object TDReader extends BSONDocumentReader[CustomerDictionary] {
    def read(doc: BSONDocument): CustomerDictionary = CustomerDictionary(
      doc.getAs[String]("segment_id").get,
      doc.getAs[String]("segment_type").get,
      doc.getAs[String]("segment_name").get,
      doc.getAs[String]("sql").get,
      doc.getAs[String]("update_frequency").get,
      doc.getAs[String]("detail").get,
      doc.getAs[String]("description").get,
      doc.getAs[String]("create_time").get,
      doc.getAs[String]("update_time").get,
      doc.getAs[String]("creator").get,
      doc.getAs[Boolean]("is_focus").get,
      doc.getAs[List[String]]("tickets").toList.flatten)
  }
}
