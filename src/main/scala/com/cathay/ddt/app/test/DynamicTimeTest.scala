package com.cathay.ddt.app.test

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cathay.ddt.ats.TagState.Monthly
import com.cathay.ddt.utils.CalendarConverter

object DynamicTimeTest extends App with CalendarConverter {

//  def getCurrentDate(a: String): String = {
////    getDateFormat(getCalendar)
//    val c = getCalendar
//    c.setTime(SDF.parse(a))
////    c.add(Calendar.MONTH, traced-1)
////    val lastDate = c.getActualMaximum(Calendar.DATE)
////    c.set(Calendar.DATE, lastDate)
//    getDateFormat(c)
//  }
//  println(getLastMonth)
//  println(getCurrentDate)
//  println(getDailyDate)


  val resetDay =
    etlTime.compareTo("00:00:00") match {
      case _ =>
        numsOfDelayDate.abs.toString
//      case _ =>
//        if((numsOfDelayDate.abs-1).toString.length == 1)
//          s"0${(numsOfDelayDate.abs-1).toString}"
//        else (numsOfDelayDate.abs-1).toString
    }

  def getRealDate(partitionValue: String): String = {
    partitionValue match {
      case x if x == getCurrentMonth => getDailyDate

      case x if x == getLastMonth =>
        if(getCurrentDate == getDayOfMonth(1)) getDailyDate
        else getLastDayOfMonth(partitionValue)

      case _ => getLastDayOfMonth(partitionValue)
    }
  }

  def getAdwDay(partitionValue: String): String = {
    val c = getCalendar
    c.setTime(SMF.parse(getDailyDate.split("-").dropRight(1).mkString("")))
    val pv = getCalendar
    pv.setTime(SMF.parse(partitionValue))

    if(pv.compareTo(c) == 0) {
      getDailyDate
    }else if(pv.before(c)) {
      getLastDayOfMonth(partitionValue)
    }else null
  }

//  pv.add(Calendar.MONTH, -1)

  //  val c = getCalendar
//  c.setTime(SMF.parse("201804"))
//
//  val b = getCalendar
//  b.setTime(SMF.parse("201804"))
//  b.add(Calendar.MONTH, -1)

  val pv = getCalendar
  pv.add(Calendar.DATE, numsOfDelayDate)

  println(resetDay)
  println(numsOfDelayDate.abs)

  println(numsOfDelayDate.abs-1)
}