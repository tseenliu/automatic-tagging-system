package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.CustomerDictionary
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.{BSONDocument, Macros}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by Tse-En on 2017/12/12.
  */
object MongoUtils {
  implicit val reader = Macros.reader[CustomerDictionary]
  implicit val writer = Macros.writer[CustomerDictionary]


  /* Write Documents */
  def insertDoc(coll: BSONCollection, doc: BSONDocument): Future[Boolean] = {
    val writeRes: Future[WriteResult] = coll.insert(doc)

    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => e.printStackTrace()
      case Success(writeResult) =>
        println(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(result => result.ok)
    //writeRes.map(_ => {}) // in this example, do nothing with the success
  }

  def insert(coll: BSONCollection, td: CustomerDictionary): Future[Boolean] = {
    val writeRes: Future[WriteResult] = coll.insert(td)
    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => e.printStackTrace()
      case Success(writeResult) =>
        println(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(_.ok)
  }

  def remove(coll: BSONCollection, selector: BSONDocument): Future[Boolean] = {
    val futureRemove = coll.remove(selector /*, firstMatchOnly = true*/)

    futureRemove.onComplete { // callback
      case Failure(e) => throw e
      case Success(writeResult) => println("successfully removed document")
    }
    futureRemove.map(_.ok)
  }




  /* Find Documents */
  def findDictionaries(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[CustomerDictionary]] = {
    implicit val reader = Macros.reader[CustomerDictionary]
    collection.find(query).cursor[CustomerDictionary]().collect[List]()
  }

  def findOneDictionary(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[CustomerDictionary] = {
    implicit val reader = Macros.reader[CustomerDictionary]
    collection.find(query).requireOne[CustomerDictionary]
  }

}
