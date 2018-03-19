package com.cathay.ddt.db.example

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/15.
  */
object Query extends App {

  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag")

  val query2 = BSONDocument("_id" -> BSONObjectID("5aa7378801000001002601bd"))
  FBsonCollection.flatMap(x => MongoUtils.findOneDictionary(x, query2)).map { docList =>
    println(docList)
  }

}
