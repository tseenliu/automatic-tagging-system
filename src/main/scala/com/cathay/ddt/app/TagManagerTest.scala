package com.cathay.ddt.app

import akka.persistence._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.cathay.ddt.ats.TagManager.{Cmd, Load}
import com.cathay.ddt.ats._
import com.cathay.ddt.tagging.schema.{TagDictionary, TagMessage}


/**
  * Created by Tse-En on 2017/12/21.
  */
object TagManagerTest extends App with EnvLoader {

//  val system = ActorSystem("persistent-actors")
//
//  val tagManager = system.actorOf(Props[TagManager])

//  val tagDic = TagDictionary(
//    channel_type = "bank",
//    tag_type = "刷卡消費",
//    tag_name = "超市購物",
//    sql = """
//            |select
//            |cutsomerID,
//            |cnt,
//            |item,
//            |txn_amt,
//            |txn_date
//            |from travel
//            |where txn_date between date_add(cast('$startDate' as date), -$numDays) AND '$startDate'
//          """.stripMargin.trim,
//    update_frequency = "d",
//    started = s"-4".toInt,
//    traced = s"4".toInt,
//    description = "超市購買族群",
//    enable_flag = true,
//    score_option = "A",
//    attribute = "behavior",
//    creator = "Alex")
  //tagManager ! Cmd(Load(tagDic))
  //tagManager ! "print"

  val kafkaConfig = getConfig("kafka")
  val tagManagerRef = TagManager.initiate(kafkaConfig)
//  TagManager.exportToRegistries(tagManagerRef)
  tagManagerRef ! "print"

//  SqlParser.print()
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))

}
