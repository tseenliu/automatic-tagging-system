package com.cathay.ddt.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by Tse-En on 2018/1/14.
  */
trait CalendarConverter extends EnvLoader {

  val config = getConfig("hive")

  val SMF = new SimpleDateFormat("yyyyMM")
  val SDF = new SimpleDateFormat("yyyyMMdd")

  val numsOfDelayDate = config.getString("hive.calendar.daily").toInt
  val numsOfDelayMonth = config.getString("hive.calendar.monthly").toInt

  def getCalendar = Calendar.getInstance()
  def getDateFormat(c: Calendar) = SDF.format(c.getTime)
  def getMonthFormat(c: Calendar) = SMF.format(c.getTime)
  def getCurrentMonth = getMonthFormat(getCalendar)
  def getCurrentDate = getDateFormat(getCalendar)
  def getDayOfMonth(day: Int) = {
    val c = Calendar.getInstance()
    c.set(Calendar.DAY_OF_MONTH, day)
    getDateFormat(c)
  }

  // t-1 month
  def getLastMonth = {
    val c = getCalendar
    c.add(Calendar.MONTH, numsOfDelayMonth)
    getMonthFormat(c)
  }

  // t-2 day
  def getDailyDate: String = {
    val c = getCalendar
    c.add(Calendar.DATE, numsOfDelayDate)
    getDateFormat(c)
  }

  // t-(>1) month
  def getDailyDate(partitionValue: String) = {
    val c = getCalendar
    c.setTime(SMF.parse(partitionValue))
    val lastDate = c.getActualMaximum(Calendar.DATE)
    c.set(Calendar.DATE, lastDate)
    getDateFormat(c)
  }

  def getStartWithDate(started: Int): String = {
    val c = getCalendar
    c.setTime(SDF.parse(getDailyDate))
    c.add(Calendar.DATE, started)
    getDateFormat(c)
  }

  def getEndWithDate(startDay: String, traceDay: Int)= {
    val c = getCalendar
    c.setTime(SDF.parse(startDay))
    c.add(Calendar.DATE, traceDay)
    getDateFormat(c)
  }

}
