package com.cathay.ddt.app.test

import com.cathay.ddt.utils.YarnMetricsChecker
object YarnTest extends App {

  val tmp = YarnMetricsChecker.YARN_MASTERS.flatMap{ x =>
    try { YarnMetricsChecker.getChecker.getYarnMetrics(x) }
    catch { case foo: java.lang.IndexOutOfBoundsException => None }
  }.head

//  val tmp = YarnMetricsChecker.getChecker.getYarnMetrics.get
  println(tmp)
  println(tmp.availableMB)
  println(tmp.availableVirtualCores)
  println(tmp.getTotalInstance)
  println(tmp.getTotalVirtualCores)
  println(tmp.getTotalMB)


}
