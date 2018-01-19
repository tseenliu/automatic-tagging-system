package com.cathay.ddt.app

import com.cathay.ddt.ats.TagManager.{Cmd, ShowState}
import com.cathay.ddt.ats._
import com.cathay.ddt.utils.EnvLoader


/**
  * Created by Tse-En on 2017/12/21.
  */
object TagManagerTest extends App with EnvLoader {

  val kafkaConfig = getConfig("kafka")
  val tagManagerRef = TagManager.initiate(kafkaConfig)
  Thread.sleep(5000)
  tagManagerRef ! Cmd(ShowState)

//  SqlParser.print()
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))

}
