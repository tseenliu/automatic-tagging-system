package com.cathay.ddt.db.example

import com.cathay.ddt.ats.TagManager
import com.cathay.ddt.ats.TagManager._
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/15.
  */
object Query extends App {

  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag")

  //val tmp = TagManager.initiate()

  val query = BSONDocument("attribute" -> "behavior")
  FBsonCollection.flatMap( scoreTagColl=> MongoUtils.getScoreTDs(scoreTagColl, query)).map { docList =>
    for ( doc <- docList) {
      println(doc)
      //println(doc.actorID)
      //tmp ! Cmd(TagAdded(doc))
    }
  }

  //  val transql = FBsonCollection.flatMap( personColl=> MongoUtils.findOne(personColl, query)).map { doc =>
  //
  //    val id = doc.get.getAs[BSONObjectID]("_id").getOrElse(BSONObjectID)
  //    val tag = doc.get.getAs[BSONString]("tag").getOrElse(BSONString(null)).value
  //    val sql = doc.get.getAs[BSONString]("sql").getOrElse(BSONString(null)).value
  //    val start = doc.get.getAs[BSONInteger]("start").getOrElse(BSONInteger(0)).value
  //    val trace = doc.get.getAs[BSONInteger]("trace").getOrElse(BSONInteger(0)).value
  //    sql.replaceAll("\\'\\$startDate\\'","\"2017-03-27\"").replaceAll("\\$numDays", trace.toString)
  //  }
  ////  transql.map(x => Sparkquery.run(x).show() )
  //  transql.map(x => println(x))



   // Remove a Document
//      FBsonCollection.flatMap( personColl => MongoUtils.remove(personColl, query)).onSuccess {
//        case result =>
//          println(s"successfully or not: $result")
//      }

  //FBsonCollection.map(x => println(x.db.name))

}
