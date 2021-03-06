package com.cathay.ddt.db

import com.cathay.ddt.utils.EnvLoader
import reactivemongo.api.{MongoConnection, MongoDriver}
import reactivemongo.api.collections.bson.BSONCollection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by Tse-En on 2017/12/12.
  */

object MongoConnector extends EnvLoader{
  val tagConfig = getConfig("ats")
  val MONGO_SERVER = tagConfig.getString("ats.mongo.current.server")
  val MONGO_PORT = tagConfig.getString("ats.mongo.current.port")
  val ATS_DB = tagConfig.getString("ats.mongo.current.db-name")
  val TAG_DICT= tagConfig.getString("ats.mongo.current.tag-collection")

  val HMONGO_SERVER = tagConfig.getString("ats.mongo.history.server")
  val HMONGO_PORT = tagConfig.getString("ats.mongo.history.port")
  val HATS_DB = tagConfig.getString("ats.mongo.history.db-name")
  val HTAG_DICT= tagConfig.getString("ats.mongo.history.tag-collection")


//  val CUS_DICT= tagConfig.getString("ats.mongo.customer-collection")

  val driver: MongoDriver = new MongoDriver
  val connection: MongoConnection = driver.connection(List(MONGO_SERVER,MONGO_PORT))

  def getTDCollection = dbFromConnection(connection, ATS_DB, TAG_DICT)
//  def getTDCollection = dbFromConnection(connection, ATS_DB, CUS_DICT)

  val hConnection: MongoConnection = driver.connection(List(HMONGO_SERVER,HMONGO_PORT))
  def getHTDCollection = dbFromConnection(hConnection, HATS_DB, HTAG_DICT)

  def dbFromConnection(connection: MongoConnection, database: String, collection: String): Future[BSONCollection] =
    connection.database(database).
      map(_.collection(collection))
}
