package com.cathay.ddt.app

import com.cathay.ddt.ats.TagManager.{Cmd, ShowState}
import com.cathay.ddt.ats._
import com.cathay.ddt.utils.EnvLoader
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Tse-En on 2017/12/21.
  */
object BootTagManager extends App with EnvLoader {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemName = "ATS"

  try
  {
    val tagManagerRef = TagManager.initiate
    Thread.sleep(5000)
    tagManagerRef ! Cmd(ShowState)
  }
  catch
    {
      case err: Throwable => logger.error(err.toString)
    }


}
