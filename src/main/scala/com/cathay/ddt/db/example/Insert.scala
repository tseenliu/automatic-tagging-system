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

  // where txn_date between date_add(cast('$startDate' as date), -$numDays) AND '$startDate'
  "event_cc_txn"
  "party_drv"
  "event_bp_point"
  "party_drv_vip"
  "rd_mis_merchant_id"
  "rd_mis_mcc_code"

  for ( i <- 1 to 3 ) {
      val tmp = TagDictionary(
        channel_type = "bank",
        channel_name = "信用卡/簽帳卡",
        tag_type = "刷卡消費",
        tag_name = "超市購物",
        sql = """
                |select
                |cutsomerID,
                |cnt,
                |item,
                |txn_amt,
                |txn_date
                |from vp_bank.event_cc_txn
                |join vp_bank.event_bp_point
                |join vp_bank.rd_mis_merchant_id
                |join vp_bank.rd_mis_mcc_code
                |where yyyymm between concat(substr('''$start_date''',1,4),substr('''$start_date''',6,2))
                |AND concat(substr('''$end_date''',1,4),substr('''$end_date''',6,2))
              """.stripMargin.trim,
        update_frequency = "M",
        started = s"-$i".toInt,
        traced = s"$i".toInt,
        description = "超市購買族群",
        enable_flag = true,
        score_option = "C",
        attribute = "behavior",
        creator = "Alex",
        is_focus = true)

      FBsonCollection.flatMap( coll => MongoUtils.insert(coll, tmp) ).onSuccess {
        case result =>
          println(s"successfully or not: $result")
      }

  }


  for ( i <- 10 to 20 ) {
    if (i % 10 == 0) {
      val tmp = TagDictionary(
        channel_type = "bank",
        channel_name = "信用卡/簽帳卡",
        tag_type = "刷卡消費",
        tag_name = "超市購物",
        sql = """
                |select
                |cutsomerID,
                |cnt,
                |item,
                |txn_amt,
                |txn_date
                |join vp_bank.party_drv
                |join vp_bank.party_drv_vip
                |join vp_bank.rd_mis_merchant_id
                |where txn_date between $start_date AND $end_date
              """.stripMargin.trim,
        update_frequency = "D",
        started = s"-$i".toInt,
        traced = s"$i".toInt,
        description = "超市購買族群",
        enable_flag = true,
        score_option = "C",
        attribute = "behavior",
        creator = "Alex",
        is_focus = true)

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
//         |where yyyymm between concat(substr('''$end_date''',1,4),substr('''$end_date''',6,2))
//         |AND concat(substr('''$start_date''',1,4),substr('''$start_date''',6,2))
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