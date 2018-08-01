package com.cathay.ddt.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cathay.ddt.ats.SegmentState.{Daily, FrequencyType, Monthly}
import com.typesafe.config.Config

/**
  * Created by Tse-En on 2018/1/14.
  */
trait CalendarConverter extends EnvLoader {

  val config: Config = getConfig("ats")
  val SMF = new SimpleDateFormat("yyyyMM")
  val SDF = new SimpleDateFormat("yyyy-MM-dd")

  val numsOfDelayDate: Int = config.getInt("ats.hive.scheduler.daily")
  val numsOfDelayMonth: Int = config.getInt("ats.hive.scheduler.monthly")
  val etlTime: String = config.getString("ats.hive.scheduler.time")
  val setCurrentDay: Boolean = config.getBoolean("ats.hive.set-currentday")
  val currentDay: String = config.getString("ats.hive.currentday")

  def getDateFormat(c: Calendar): String = SDF.format(c.getTime)
  def getMonthFormat(c: Calendar): String = SMF.format(c.getTime)
  def getCurrentMonth: String = getMonthFormat(getCalendar)
  def getDayOfMonth(day: Int): String = {
    val c = Calendar.getInstance()
    c.set(Calendar.DAY_OF_MONTH, day)
    getDateFormat(c)
  }

  // t-1 month
  def getLastMonth: String = {
    if(getCurrentDate.split("-")(2) == s"0${numsOfDelayDate.abs.toString}") {
      val c = getCalendar
      c.add(Calendar.MONTH, numsOfDelayMonth)
      getMonthFormat(c)
    }else {
      val c = getCalendar
      c.add(Calendar.DATE, numsOfDelayDate)
      c.add(Calendar.MONTH, numsOfDelayMonth)
      getMonthFormat(c)
    }
  }

  def getDailyDate: String = {
    val c = getCalendar
    // t-2 day
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
        c.add(Calendar.MONTH, started+1)
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

  def getCurrentDate: String = {
    if(setCurrentDay) {
      val c = getCalendar
      c.setTime(SDF.parse(currentDay))
      getDateFormat(c)
    }else getDateFormat(getCalendar)
  }

  def getCalendar: Calendar = {
    if (setCurrentDay) {
      val c = Calendar.getInstance()
      c.setTime(SDF.parse(currentDay))
      c
    }else Calendar.getInstance()
  }


}
