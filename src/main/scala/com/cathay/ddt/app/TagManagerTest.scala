package com.cathay.ddt.app

import com.cathay.ddt.ats.TagManager.{Cmd, ShowState}
import com.cathay.ddt.ats._
import com.cathay.ddt.utils.EnvLoader


/**
  * Created by Tse-En on 2017/12/21.
  */
object TagManagerTest extends App with EnvLoader {

  val tagManagerRef = TagManager.initiate
  Thread.sleep(5000)
  tagManagerRef ! Cmd(ShowState)

//  Thread.sleep(15000)
//  tagManagerRef ! Cmd(Delete("5a59f9a90100000100d02159"))
//  Thread.sleep(300)
//  tagManagerRef ! Cmd(ShowState)
  //Delete Check

//  SqlParser.print()
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))
//  println(SqlParser.getMappingTable("event_bp_point"))

}
