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
  val MONGO_SERVER = tagConfig.getString("ats.mongo.server")
  val ATS_DB = tagConfig.getString("ats.mongo.db-name")
  val TAG_DICT= tagConfig.getString("ats.mongo.tag-collection")

  val driver: MongoDriver = new MongoDriver
  val connection: MongoConnection = driver.connection(List(MONGO_SERVER))

  def getTDCollection = dbFromConnection(connection, ATS_DB, TAG_DICT)

  def dbFromConnection(connection: MongoConnection, database: String, collection: String): Future[BSONCollection] =
    connection.database(database).
      map(_.collection(collection))
}
