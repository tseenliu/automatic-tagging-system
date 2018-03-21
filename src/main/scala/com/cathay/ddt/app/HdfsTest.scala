package com.cathay.ddt.app

import com.cathay.ddt.tagging.schema.{TagDictionary, TagType}
import com.cathay.ddt.utils.{HdfsWriter, ViewMapper}
import spray.json._

object HdfsTest extends App {

  println(HdfsWriter.URL)
  println(HdfsWriter.TMP_FILE_PATH)

  println(ViewMapper.getViewMapper.initial())

  // tag output
  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
  val neilYoung = TagDictionary(
    tag_id = randomString(20),
    channel_type = "bank",
    channel_item = "信用卡/簽帳卡",
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
    is_focus = true)


  import com.cathay.ddt.tagging.schema.TagDictionaryProtocol._

  val json = neilYoung.toJson
  println(json.prettyPrint)

  HdfsWriter.write(fileName = "20180321", data = json.prettyPrint.getBytes)
}
