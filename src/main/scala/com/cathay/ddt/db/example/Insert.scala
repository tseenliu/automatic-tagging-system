package com.cathay.ddt.db.example

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.tagging.schema.TagDictionary

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Tse-En on 2017/12/12.
  */

object Insert extends App {
  val connection1 = MongoConnector.connection
  val FBsonCollection = MongoConnector.dbFromConnection(connection1, "tag", "icustomer")

  for (i <- 4 to 5) {
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
      started = None,
      traced = None,
      description = "超市購買族群",
      enable_flag = true,
      score_option = "C",
      attribute = "behavior",
      creator = "Jenny",
      is_focus = true)

      FBsonCollection.flatMap( coll => MongoUtils.insert(coll, tmp) ).onSuccess {
        case result =>
          println(s"successfully or not: $result")
      }

  }

}