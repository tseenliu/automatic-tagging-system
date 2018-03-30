package com.cathay.ddt.db.example

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/15.
  */
object Query extends App {

  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag_2")

  val query1 = BSONDocument("_id" -> BSONObjectID("5ab4b8df785d10d1c574646b"))
  val query2 = BSONDocument("tag_name" -> "子女教育")
  FBsonCollection.flatMap(x => MongoUtils.findOneDictionary(x, query1)).map { docList =>
    println(docList)
  }

}
