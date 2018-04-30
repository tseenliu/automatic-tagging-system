package com.cathay.ddt.utils

import java.sql.{Connection, DriverManager, Statement}

import com.cathay.ddt.tagging.schema.TagMessage
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

class ViewMapper {
  import ViewMapper._
  val driverName = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  def initial(): Unit = {
    val con: Connection = DriverManager.getConnection(s"jdbc:hive2://$URL", HIVE_USER, HIVE_USER_PASS)
    val stmt: Statement = con.createStatement()

    val ParsingSQLCommand: String = {
      s"""
         |select
         |view_id, db_id, table_id, year_ind, month_ind, day_ind, ref_ind, partition_id
         |from $VIEW_TABLE
        """.stripMargin
      }

    val res = stmt.executeQuery(ParsingSQLCommand)

    while (res.next()) {
//      if(res.getString(1).contains(".")) {
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
        val partition = res.getString(8)

        if(ref == "v") addItem("D", view, s"$db.$table", partition)
        else if (month == "v" && day == "v") addItem("D", view, s"$db.$table", partition)
        else if(month == "v") addItem("M", view, s"$db.$table", partition)
        else if(day == "v") addItem("D", view, s"$db.$table", partition)
    }
  }

  def addItem(frequency: String, view: String, dbTable: String, partition: String) {
    kafkaMList += ((dbTable, frequency))
    sqlMList += ((view, SimpleTagMessage(frequency, dbTable, partition)))
  }

  def getKafkaMList: ListBuffer[(String, String)] = kafkaMList
  def getSqlMList: ListBuffer[(String, TagMessage.Message)] = sqlMList
}

object ViewMapper extends EnvLoader {
  private final val viewMapper = new ViewMapper
  def getViewMapper: ViewMapper = viewMapper

  val config: Config = getConfig("ats")
  val HIVE_SERVER = config.getString("ats.hive.server")
  val THRIFT_PORT = config.getString("ats.hive.thrift-port")
  val URL = HIVE_SERVER + ":" + THRIFT_PORT
  val HIVE_USER = config.getString("ats.hive.user")
  val HIVE_USER_PASS = config.getString("ats.hive.password")
  val VIEW_TABLE = config.getString("ats.hive.db-table")

  val sqlMList = new ListBuffer[(String, Message)]()
  val kafkaMList = new ListBuffer[(String, String)]()
}
