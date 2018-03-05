package com.cathay.ddt.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cathay.ddt.ats.TagState.{Daily, FrequencyType, Monthly}
import com.typesafe.config.Config

/**
  * Created by Tse-En on 2018/1/14.
  */
trait CalendarConverter extends EnvLoader {

  val config: Config = getConfig("hive")
  val SMF = new SimpleDateFormat("yyyyMM")
  val SDF = new SimpleDateFormat("yyyy-MM-dd")

  val numsOfDelayDate: Int = config.getString("hive.scheduler.daily").toInt
  val numsOfDelayMonth: Int = config.getString("hive.scheduler.monthly").toInt
  val etlTime: String = config.getString("hive.scheduler.time")

  def getCalendar: Calendar = Calendar.getInstance()
  def getDateFormat(c: Calendar): String = SDF.format(c.getTime)
  def getMonthFormat(c: Calendar): String = SMF.format(c.getTime)
  def getCurrentMonth: String = getMonthFormat(getCalendar)
  def getCurrentDate: String = getDateFormat(getCalendar)
  def getDayOfMonth(day: Int): String = {
    val c = Calendar.getInstance()
    c.set(Calendar.DAY_OF_MONTH, day)
    getDateFormat(c)
  }

  // t-1 month
  def getLastMonth: String = {
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
  def getLastDayOfMonth(partitionValue: String): String = {
    val c = getCalendar
    c.setTime(SMF.parse(partitionValue))
    val lastDate = c.getActualMaximum(Calendar.DATE)
    c.set(Calendar.DATE, lastDate)
    getDateFormat(c)
  }

  def getStartDate(frequencyType: FrequencyType, started: Int): String = {
    frequencyType match {
      case Monthly =>
        val c = getCalendar
        c.setTime(SMF.parse(getLastMonth))
        c.set(Calendar.MONTH, started)
        getDateFormat(c)
      case Daily =>
        val c = getCalendar
        c.setTime(SDF.parse(getDailyDate))
        c.add(Calendar.DATE, started+1)
        getDateFormat(c)
    }
  }

  def getEndDate(frequencyType: FrequencyType, started: String, traced: Int): String = {
    frequencyType match {
      case Monthly =>
        val c = getCalendar
        c.setTime(SDF.parse(started))
        c.add(Calendar.MONTH, traced-1)
        val lastDate = c.getActualMaximum(Calendar.DATE)
        c.set(Calendar.DATE, lastDate)
        getDateFormat(c)
      case Daily =>
        val c = getCalendar
        c.setTime(SDF.parse(started))
        c.add(Calendar.DATE, traced-1)
        getDateFormat(c)
    }
  }

}
