package com.cathay.ddt.db.example

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.tagging.schema.TagDictionary
import com.cathay.ddt.tagging.{Sparkquery, example}
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/12.
  */

object Insert extends App {
  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag")

  for ( i <- 10 to 30 ) {
    if (i % 10 == 0) {
      val tmp = TagDictionary(
        channel_type = "bank",
        tag_type = "刷卡消費",
        tag_name = "超市購物",
        sql = """
                |select
                |cutsomerID,
                |cnt,
                |item,
                |txn_amt,
                |txn_date
                |from travel
                |where txn_date between date_add(cast('$startDate' as date), -$numDays) AND '$startDate'
              """.stripMargin.trim,
        update_frequency = "d",
        started = s"-$i".toInt,
        traced = s"$i".toInt,
        description = "超市購買族群",
        enable_flag = true,
        score_option = "A",
        attribute = "behavior",
        creator = "Alex")

      FBsonCollection.flatMap( coll => MongoUtils.insert(coll, tmp) ).onSuccess {
        case result =>
          println(s"successfully or not: $result")
      }
    }

  }

//  val tag = BSONDocument(
//    "_id" -> BSONObjectID.generate,
//    "tag" -> "travel",
//    "sql" ->
//      """
//         |select
//         |cutsomerID,
//         |cnt,
//         |item,
//         |txn_amt,
//         |txn_date
//         |from travel
//         |where txn_date between date_add(cast('$startDate' as date), -$numDays) AND '$startDate'
//       """.stripMargin.trim,
//    "md" -> "m",
//    "started" -> -2,
//    "traced" -> 1,
//    "option" -> "B")
//
//  // Write Documents
//  FBsonCollection.flatMap(x => MongoUtils.insertDoc(x, tag)).onSuccess {
//    case result =>
//      println(s"successfully or not: $result")
//  }

}