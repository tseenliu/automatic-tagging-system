package com.cathay.ddt.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsClient extends CalendarConverter {

  val hdfsConfig = getConfig("ats")
  val HADOOP_USER_NAME = hdfsConfig.getString("ats.hdfs.hadoop-user-name")
  val URL = s"${hdfsConfig.getString("ats.hdfs.namenode-host")}:${hdfsConfig.getString("ats.hdfs.namenode-port")}"
  val TMP_FILE_PATH = {
    val path = hdfsConfig.getString("ats.hdfs.output-hdfsDir")
    if(path.last == '/') path
    else path + '/'
  }

  def write(uri: String = URL, filePath: String = TMP_FILE_PATH, fileName: String, data: Array[Byte]) = {
    var status: Boolean = false
    System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME)
    val path = new Path(filePath + fileName)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    if (fs.exists(path)) status = true
    else status = false
    fs.close()
    status
  }

  def delete(uri: String = URL, filePath: String = TMP_FILE_PATH, fileName: String): Boolean = {
    var status: Boolean = false
    System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME)
    val path = new Path(filePath + fileName)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    if(fs.exists(path)) {
      status = fs.delete(path, false)
    }
    fs.close()
    status
  }

}

object HdfsClient {
  private final val HDFSCLIENT = new HdfsClient
  def getClient: HdfsClient = HDFSCLIENT
}
