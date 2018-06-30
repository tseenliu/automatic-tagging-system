package com.cathay.ddt.app.test

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/15.
  */
object Query extends App {

  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag_api")

  val query1 = BSONDocument("_id" -> BSONObjectID("5ab4b8df785d10d1c574646b"))


  val query3 = BSONDocument("tag_name" -> "超市購物")


//  val query = BSONDocument("tag_type" -> BSONDocument("type_L1" -> "D", "type_L2" -> "d"))
//  val query = BSONDocument("tag_type" -> BSONDocument(tmpList))

  val query = BSONDocument(
    "source_type" -> "bank",
    "source_item" -> "信用卡/簽帳卡",
    "tag_type" -> BSONArray(BSONDocument("type_L1" -> "A", "type_L2" -> "b")),
    "tag_name" -> "超市購物",
    "update_frequency" -> "M",
    "started" -> -6,
    "traced" -> 6,
    "score_method" -> "Z",
    "attribute" -> "behavior",
    "system_name" -> "ATS"
  )

//  "source_type": "bank",
//  "source_item": "信用卡/簽帳卡",
//  "tag_type": [
//  {
//    "type_L1": "A",
//    "type_L2": "b"
//  }
//  ],
//  "tag_name": "超市購物",
//  "update_frequency": "M",
//  "started": -6,
//  "traced": 6,
//  "score_method": "Z",
//  "attribute": "behavior",
//  "system_name": "ATS"

//  val query4 = BSONDocument("tag_name" -> "超市購物", "$and" -> BSONArray(BSONDocument("type_L1" -> "D", "type_L2" -> "d"), BSONDocument("type_L1" -> "D", "type_L2" -> "d")))

//  val query4 = BSONDocument("tag_name" -> "超市購物", "tag_type" -> BSONArray())

//  var query0 = BSONDocument()
//  query0 ++= BSONDocument("tag_name" -> "超市購物")
//  query0 ++= BSONDocument("update_frequency" -> "M")
//
//  println(query0)
//  FBsonCollection.flatMap(x => MongoUtils.findOneDictionary(x, query0)).map { docList =>
//    println(docList)
//  }

  FBsonCollection.flatMap(coll => MongoUtils.remove(coll, BSONDocument("tag_id" -> "123"))).onSuccess {
    case result =>
      println(s"successfully or not: $result")
  }

}
