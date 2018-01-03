package com.cathay.ddt.ats

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Tse-En on 2017/12/25.
  */
trait EnvLoader {

  var configDir: String = "config"

  def getConfig(name: String): Config =
    ConfigFactory.parseFile(new File(s"${this.configDir}/$name.conf"))
}

