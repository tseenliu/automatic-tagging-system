package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}
import com.cathay.ddt.ats.TagState.{Daily, Monthly}
import com.cathay.ddt.kafka.{MessageConsumer, MessageProducer}
import com.cathay.ddt.tagging.schema.TagDictionary
import com.cathay.ddt.utils.EnvLoader

/**
  * Created by Tse-En on 2017/12/26.
  */
object kafkaTest extends App with EnvLoader {

  //consumer
  val system = ActorSystem("alex")
  val kafkaActor = system.actorOf(Props(new MessageConsumer), "kafka-test")


  val dic = TagDictionary(
    tag_id = "asfsdregfgdsgsdfgdsgert1345353454363redfsf34g",
    channel_type = "bank",
    channel_item = "信用卡/簽帳卡",
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
    started = Option(-6),
    traced = Option(6),
    description = "超市購買族群",
    disable_flag = Option(false),
    create_time = "2018-03-12",
    update_time = "2018-03-12",
    score_method = "C",
    attribute = "behavior",
    creator = "Jenny",
    is_focus = true)

  //producer
  MessageProducer.getProducer.sendToFinishTopic(Monthly, dic)
  Thread.sleep(5000)
  MessageProducer.getProducer.sendToFinishTopic(Daily, dic)
}
