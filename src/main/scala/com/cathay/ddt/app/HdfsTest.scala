package com.cathay.ddt.app

import com.cathay.ddt.tagging.schema.{ComposeTD, TagDictionary, TagType}
import com.cathay.ddt.utils.{HdfsClient, ViewMapper}
import spray.json._

object HdfsTest extends App {

  // tag output
  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
  val neilYoung1 = TagDictionary(
    tag_id = randomString(20),
    source_type = "bank",
    source_item = "信用卡/簽帳卡",
    tag_type = List(TagType("D","d"), TagType("E","e")),
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
    disable_flag = Option(false),
    create_time = "2018-03-12",
    update_time = "2018-03-12",
    score_method = "Z",
    attribute = "behavior",
    creator = "Jasmine",
    is_focus = true,
    system_name = "ATS")

  val neilYoung = ComposeTD(
    tag_id = randomString(20),
    source_type = "bank",
    source_item = "信用卡/簽帳卡",
    tag_type = List(TagType("D","d"), TagType("E","e")),
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
    started = Some(-6),
    traced = Some(6),
    score_method = "Z",
    attribute = "behavior",
    start_date = Some("2018-01-01"),
    end_date = Some("2018-02-28"),
    execute_date = "2018-03-23",
    system_name = "ATS")


  import com.cathay.ddt.tagging.schema.ComposeTDProtocol._

  val json = neilYoung.toJson
  println(json.compactPrint)

  HdfsClient.getClient.write(fileName = "test.json", data = json.compactPrint.getBytes)
//  val a = HdfsClient.delete(fileName = "test.json")
//  println(s"status: $a")
}
