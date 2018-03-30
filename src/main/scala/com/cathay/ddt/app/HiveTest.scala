package com.cathay.ddt.app

import java.sql.{Connection, DriverManager}

import com.cathay.ddt.utils.ViewMapper.{HIVE_USER, HIVE_USER_PASS, URL, VIEW_TABLE}

object HiveTest extends App {

  val driverName = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val con: Connection = DriverManager.getConnection(s"jdbc:hive2://$URL", HIVE_USER, HIVE_USER_PASS)
//  val con = DriverManager.getConnection("jdbc:hive2://parhhdpm2:10000", "pdt30802", "cathay")
  val stmt = con.createStatement()
  val ParsingSQLCommand: String = {
    s"""
       |select
       |view_id, db_id, table_id, year_ind, month_ind, day_ind, ref_ind
       |from $VIEW_TABLE
    """.stripMargin
  }
//  val ParsingSQLCommand = s"""
//                |select
//                |view_id, db_id, table_id, year_ind, month_ind, day_ind, ref_ind
//                |from btmp.etl_adw_table
//    """.stripMargin
  val res = stmt.executeQuery(ParsingSQLCommand)
  while (res.next()) {
    println(res.getString(1)
      + "\t" + res.getString(2)
      + "\t" + res.getString(3)
      + "\t" + res.getString(4)
      + "\t" + res.getString(5)
      + "\t" + res.getString(6)
      + "\t" + res.getString(7))
  }
}
