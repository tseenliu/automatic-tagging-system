package com.cathay.ddt.app

import com.cathay.ddt.utils.YarnMetricsChecker
object YarnTest extends App {

  val tmp = YarnMetricsChecker.getChecker.getYarnMetrics.get
  println(tmp)
  println(tmp.availableMB)
  println(tmp.availableVirtualCores)
  println(tmp.getTotalInstance)
  println(tmp.getTotalVirtualCores)
  println(tmp.getTotalMB)


}
