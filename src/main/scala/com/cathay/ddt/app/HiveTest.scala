package com.cathay.ddt.app

import java.sql.DriverManager

object HiveTest extends App {

  val driverName = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val con = DriverManager.getConnection("jdbc:hive2://parhhdpm2:10000/btmp", "pdt30802", "cathay")
  val stmt = con.createStatement()
  val sql3 = s"""
                |select
                |view_id, db_id, table_id, year_ind, month_ind, day_ind, ref_ind
                |from etl_adw_table
    """.stripMargin
  val res = stmt.executeQuery(sql3)
  while (res.next()) {
    println(res.getString(1)
      + "\t" + res.getString(2)
      + "\t" + res.getString(3)
      + "\t" + res.getString(4)
      + "\t" + res.getString(5)
      + "\t" + res.getString(6)
      + "\t" + res.getString(7))

    val view = res.getString(1).split("\\.")(1)
    val db = res.getString(2)
    val table =  res.getString(3)
    val year = res.getString(4)
    val month = res.getString(5)
    val day = res.getString(6)
    val ref = res.getString(7)

  }
}
