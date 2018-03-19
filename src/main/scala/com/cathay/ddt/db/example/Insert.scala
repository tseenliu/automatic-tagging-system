package com.cathay.ddt.db.example

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.tagging.schema.{TagDictionary, Type}
import reactivemongo.bson.{BSON, BSONDocument}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/12.
  */

object Insert extends App {
  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "scoretag_1")

//  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
//
//  for (i <- 4 to 6) {
//    val tmp = TagDictionary(
//      tag_id = randomString(10),
//      channel_type = "bank",
//      channel_item = "信用卡/簽帳卡",
//      tag_type = "刷卡消費",
//      tag_name = "超市購物",
////      segment_type = "刷卡消費",
////      segment_name = "超市購物",
//      sql = """
//                |select
//                |cutsomerID,
//                |cnt,
//                |item,
//                |txn_amt,
//                |txn_date
//                |from vp_bank.event_cc_txn
//                |join vp_bank.event_bp_point
//                |join vp_bank.rd_mis_merchant_id
//                |join vp_bank.rd_mis_mcc_code
//                |where yyyymm between concat(substr('''$start_date''',1,4),substr('''$start_date''',6,2))
//                |AND concat(substr('''$end_date''',1,4),substr('''$end_date''',6,2))
//              """.stripMargin.trim,
//      update_frequency = "M",
//      started = Option(-i),
//      traced = Option(i),
//      description = "超市購買族群",
//      disable_flag = Option(false),
//      create_time = "2018-03-12",
//      update_time = "2018-03-12",
//      score_method = "C",
//      attribute = "behavior",
//      creator = "Jerry",
//      is_focus = true)
//
//      FBsonCollection.flatMap( coll => MongoUtils.insert(coll, tmp) ).onSuccess {
//        case result =>
//          println(s"successfully or not: $result")
//      }
//
//  }

  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

  val neilYoung = TagDictionary(
    tag_id = randomString(20),
    channel_type = "bank",
    channel_item = "信用卡/簽帳卡",
    tag_type = List(Type("D","d"), Type("E","e")),
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
    started = Option(-4),
    traced = Option(4),
    description = "超市購買族群",
    disable_flag = Option(false),
    create_time = "2018-03-12",
    update_time = "2018-03-12",
    score_method = "C",
    attribute = "behavior",
    creator = "Alex",
    is_focus = true)

//  FBsonCollection.flatMap( coll => MongoUtils.insert(coll, neilYoung) ).onSuccess {
//    case result =>
//      println(s"successfully or not: $result")
//  }

  val query = BSONDocument()
  FBsonCollection.flatMap(x => MongoUtils.findDictionaries(x, query)).map { docList =>
    docList.foreach(TD => println(TD))
  }



}