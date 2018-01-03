package com.cathay.ddt.db

import reactivemongo.api.{MongoConnection, MongoDriver}
import reactivemongo.api.collections.bson.BSONCollection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by Tse-En on 2017/12/12.
  */

object MongoConnector {
  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))

  def dbFromConnection(connection: MongoConnection, database: String, collection: String): Future[BSONCollection] =
    connection.database(database).
      map(_.collection(collection))
}
