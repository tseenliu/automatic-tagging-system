package com.cathay.ddt.utils

import com.typesafe.config.Config

import sys.process._

class YarnMetricsChecker {
  import YarnMetricsChecker._

  def getYarnMetrics(YARN_HOST: String): Option[YarnMetrics] = {
    val pattern = "[0-9]+".r
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val status = s"curl http://$YARN_HOST:$YARN_PORT/ws/v1/cluster/metrics" ! ProcessLogger(stdout append _, stderr append _)
    if(status == 0) {
      val clusterMetrics = stdout.substring(18, stdout.length-1).trim
      val Metricslist = pattern.findAllIn(clusterMetrics).toList
      Option(
        YarnMetrics(
          Metricslist(2).toInt,
          Metricslist(3)toInt,
          Metricslist(7)toInt,
          Metricslist(8)toInt,
          Metricslist(10)toInt,
          Metricslist(11)toInt,
          Metricslist(15)toInt,
          Metricslist(16)toInt
        )
      )
    }else None

  }

}

object YarnMetricsChecker extends EnvLoader{
  private final val CHECKER = new YarnMetricsChecker

  val atsConfig: Config = getConfig("ats")
  val YARN_MASTERS: Array[String] = atsConfig.getStringList("ats.yarn.resourcemanager-host").toArray().map(_.toString)
  val YARN_PORT: String = atsConfig.getString("ats.yarn.resourcemanager-port")
  val YARN_THRESHOLD: Double = atsConfig.getString("ats.yarn.resource-threshold").toDouble
  val SPARK_CORE: Int = atsConfig.getString("ats.spark.job-core").toInt
  val SPARK_MEMORY: Long = {
    val memory = atsConfig.getString("ats.spark.job-memory")
    if(List("GB", "G").exists(memory.contains(_))) {
      memory.substring(0,memory.indexOf("G")).toLong * 1024
    }else if(List("MB", "M").exists(memory.contains(_))) {
      memory.substring(0,memory.indexOf("M")).toLong
    } else {
      memory.toLong
    }
  }

  def getChecker: YarnMetricsChecker = CHECKER
  case class YarnMetrics(
                          appsPending: Int,
                          appsRunning: Int,
                          availableMB: Int,
                          allocatedMB: Int,
                          availableVirtualCores: Int,
                          allocatedVirtualCores: Int,
                          totalMB: Int,
                          totalVirtualCores: Int) {
    def getTotalMB: Double = totalMB * YARN_THRESHOLD
    def getTotalVirtualCores: Double = totalVirtualCores * YARN_THRESHOLD
    def getTotalInstance: Int = {
      (getTotalMB/SPARK_MEMORY).toInt.min((getTotalVirtualCores/SPARK_CORE).toInt)
    }
    def getAvailableInstance: Int = {
      (availableMB/SPARK_MEMORY).toInt.min((availableVirtualCores/SPARK_CORE).toInt)
    }
  }
}
