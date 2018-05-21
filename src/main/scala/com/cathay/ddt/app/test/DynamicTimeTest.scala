package com.cathay.ddt.app.test

import java.text.SimpleDateFormat
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
      case 0 =>
        numsOfDelayDate.abs.toString
      case _ =>
        if((numsOfDelayDate.abs-1).toString.length == 1)
          s"0${(numsOfDelayDate.abs-1).toString}"
        else (numsOfDelayDate.abs-1).toString
    }

  println(resetDay)
}
