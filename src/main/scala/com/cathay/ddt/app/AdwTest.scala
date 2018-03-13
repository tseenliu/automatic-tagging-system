package com.cathay.ddt.app

import com.cathay.ddt.utils.MessageConverter.{kafkaMTable, sqlMTable}
import com.cathay.ddt.utils.{MessageConverter, ViewMapper}

object AdwTest extends App {

//  println("getSqlMTable, getKafkaMTable")
//  MessageConverter.getSqlMTable
//  MessageConverter.getKafkaMTable

  println("showSqlMTable, showKafkaMTable")
  MessageConverter.printSql()
  MessageConverter.printKafka()

}
