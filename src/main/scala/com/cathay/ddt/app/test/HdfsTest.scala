package com.cathay.ddt.app.test

import com.cathay.ddt.tagging.schema.{ComposeCD, QueryCD, CustomerDictionary, TagType}
import com.cathay.ddt.utils.HdfsClient
import spray.json._

object HdfsTest extends App {

  // tag output
  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
  val neilYoung1 = CustomerDictionary(
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
    system_name = "ATS",
    tickets = List("aaa", "fvv"))

  val neilYoung = ComposeCD(
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
    score_method = "Z",
    attribute = "behavior",
    start_date = Some("2018-01-01"),
    end_date = Some("2018-02-28"),
    execute_date = "2018-03-23",
    system_name = "ATS")


  // test1
  import com.cathay.ddt.tagging.protocal.TDProtocol._
  val json2 = neilYoung1.toJson
  println(json2.prettyPrint)
  val json3 = json2.convertTo[CustomerDictionary]
  println(json3.tickets)

//  // test2
//  import com.cathay.ddt.tagging.protocal.ComposeTDProtocal._
//  val json = neilYoung.toJson
//  println(json.prettyPrint)
//  HdfsClient.getClient.write(fileName = "test.json", data = json.compactPrint.getBytes)
//  val a = HdfsClient.getClient.delete(fileName = "test.json")
//  println(s"status: $a")
//
//  // test3
//  val dynamicTD = QueryTD(
//    source_type = Some("bank"),
//    source_item = Some("信用卡/簽帳卡"),
//    tag_type = Some(List(TagType("D","d"), TagType("E","e"))),
//    tag_name = Some("超市購物"),
//    update_frequency = Some("M"),
//    started = None,
//    traced = None,
//    score_method = Some("Z"),
//    attribute = Some("behavior"),
//    system_name = Some("ATS")
//  )
//
//  import com.cathay.ddt.tagging.protocal.QueryTDProtocol._
//  val json = dynamicTD.toJson
//  println(json.prettyPrint)
//
//
//  // test4
//  val aaa = """{update_frequency: M, source_type: bank}""".toJson
//  val taaa = aaa.asInstanceOf[QueryTD]
//  println(taaa.toJson.prettyPrint)
}
