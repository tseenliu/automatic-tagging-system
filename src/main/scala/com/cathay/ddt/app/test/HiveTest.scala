package com.cathay.ddt.app.test

import java.sql.{Connection, DriverManager}

import com.cathay.ddt.utils.ViewMapper.{HIVE_USER, HIVE_USER_PASS, URL, VIEW_TABLE}

object HiveTest extends App {

  if (args.length == 0) {
    println("dude, i need at least one parameter")
  }
  val input = args(0)
  val ans = if (input == "no") null else input

  val driverName = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val con: Connection = DriverManager.getConnection(s"jdbc:hive2://$URL", HIVE_USER, HIVE_USER_PASS)
  val stmt = con.createStatement()
  val ParsingSQLCommand: String = {
    s"""
       |select
       |view_id, db_id, table_id, year_ind, month_ind, day_ind, ref_ind, partition_id, disabled_ind
       |from $VIEW_TABLE
        """.stripMargin
  }

  val res = stmt.executeQuery(ParsingSQLCommand)

  run()

  def run(): Unit = {
    while (res.next()) {
      val view =
        if(res.getString(1).contains(".")) {
          res.getString(1).split("\\.")(1)
        }else null
      val db = res.getString(2)
      val table =  res.getString(3)
      val year = res.getString(4)
      val month = res.getString(5)
      val day = res.getString(6)
      val ref = res.getString(7)
      val partition = if(res.getString(8).isEmpty) None else Some(res.getString(8))
      val disable = res.getString(9)

      disable match {
        case a if a == ans =>
          println(view
            + "\t" + db
            + "\t" + table
            + "\t" + month
            + "\t" + day
            + "\t" + ref
            + "\t" + partition
            + "\t" + disable)
        case "" =>
          println(view
            + "\t" + db
            + "\t" + table
            + "\t" + month
            + "\t" + day
            + "\t" + ref
            + "\t" + partition
            + "\t" + disable)
      }
    }
  }

}
