package com.cathay.ddt.app

import akka.actor.{ActorSystem, Props}
import com.cathay.ddt.ats.TagScheduler
import com.cathay.ddt.ats.TagScheduler._
import com.cathay.ddt.tagging.schema.TagDictionary

/**
  * Created by Tse-En on 2018/1/29.
  */
object JobSchedulerTest extends App {

  // testing
  val tag = TagDictionary(
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
    started = Option(-3),
    traced = Option(3),
    description = "超市購買族群",
    enable_flag = true,
    score_method = "C",
    attribute = "behavior",
    creator = "Jenny",
    is_focus = true)

  val scheduleInstance1 = ScheduleInstance("select * from alex", tag)
  val scheduleInstance2 = ScheduleInstance("select * from jenny", tag)
  val scheduleInstance3 = ScheduleInstance("select * from katherine", tag)



  val system = ActorSystem("Round-Robin-Router")
  val jobs = system.actorOf(Props[TagScheduler], "tag-scheduler")
  jobs ! Create
  jobs ! Schedule(scheduleInstance1)
  Thread.sleep(13000)
  jobs ! Schedule(scheduleInstance2)
  Thread.sleep(8000)
  jobs ! Schedule(scheduleInstance3)
  Thread.sleep(8000)

//  jobs ! KILL
//  Thread.sleep(3000)





  Thread.sleep(10000)
  system.terminate()
}
