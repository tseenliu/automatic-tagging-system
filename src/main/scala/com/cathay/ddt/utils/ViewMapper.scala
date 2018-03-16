package com.cathay.ddt.utils

import java.sql.{Connection, DriverManager, Statement}

import com.cathay.ddt.tagging.schema.TagMessage
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}

import scala.collection.mutable.ListBuffer

class ViewMapper {
  import ViewMapper._
  val driverName = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val con: Connection = DriverManager.getConnection("jdbc:hive2://parhhdpm2:10000/btmp", "pdt30802", "cathay")
  val stmt: Statement = con.createStatement()

  val ParsingSQLCommand: String = {
    s"""
       |select
       |view_id, db_id, table_id, year_ind, month_ind, day_ind, ref_ind
       |from etl_adw_table
    """.stripMargin
  }

  def initial(): Unit = {
    val res = stmt.executeQuery(ParsingSQLCommand)

    while (res.next()) {
      if(res.getString(1).contains(".")) {
        val view = res.getString(1).split("\\.")(1)
        val db = res.getString(2)
        val table =  res.getString(3)
        val year = res.getString(4)
        val month = res.getString(5)
        val day = res.getString(6)
        val ref = res.getString(7)

        if(ref == "v") addItem("D", view, s"$db.$table")
        else if (month == "v" && day == "v") addItem("D", view, s"$db.$table")
        else if(month == "v") addItem("M", view, s"$db.$table")
        else if(day == "v") addItem("D", view, s"$db.$table")
        else if(year == "v") addItem("Y", view, s"$db.$table")
//        else addItem("D", view, s"$db.$table")
      }
    }
  }

  def addItem(frequency: String, view: String, dbTable: String) {
    kafkaMList += ((dbTable, frequency))
    sqlMList += ((view, SimpleTagMessage(frequency, dbTable)))
  }

  def getKafkaMList: ListBuffer[(String, String)] = kafkaMList
  def getSqlMList: ListBuffer[(String, TagMessage.Message)] = sqlMList
}

object ViewMapper{
  private final val viewMapper = new ViewMapper
  def getViewMapper: ViewMapper = viewMapper

  val sqlMList = new ListBuffer[(String, Message)]()
  val kafkaMList = new ListBuffer[(String, String)]()
}
