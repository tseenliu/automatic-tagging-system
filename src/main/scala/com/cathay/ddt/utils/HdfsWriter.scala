package com.cathay.ddt.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsWriter extends CalendarConverter {

  val hdfsConfig = getConfig("ats")
  val HADOOP_USER_NAME = hdfsConfig.getString("ats.hdfs.hadoop-user-name")
  val URL = s"${hdfsConfig.getString("ats.hdfs.namenode-host")}:${hdfsConfig.getString("ats.hdfs.namenode-port")}"
  val TMP_FILE_PATH = {
    val path = hdfsConfig.getString("ats.hdfs.output-hdfsDir")
    if(path.last == '/') path
    else path + '/'
  }

  def write(uri: String = URL, filePath: String = TMP_FILE_PATH, fileName: String, data: Array[Byte]) = {
    System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME)
    val path = new Path(filePath + fileName)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
  }

}
