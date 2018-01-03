package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.TagDictionary
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
  implicit val reader = Macros.reader[TagDictionary]
  implicit val writer = Macros.writer[TagDictionary]


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

  def insert(coll: BSONCollection, td: TagDictionary): Future[Boolean] = {
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
  def getScoreTDs(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[TagDictionary]] = {
    implicit val reader = Macros.reader[TagDictionary]
    //implicit val writer = Macros.writer[ICustomer]
    collection.find(query).cursor[TagDictionary]().collect[List]()
  }

//  def getQueryOfIcustomer(): Future[List[BSONDocument]] = {
//    def query(collection: BSONCollection): Future[List[BSONDocument]] = {
//      collection.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
//    }
//  }
//
//  def QueryExecute[T](database: String, collection: String, query: BSONCollection => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
//    for {
//      db <- ReactiveMongoDao.connection.database(database)
//      result <- query(db.collection[BSONCollection](collection))
//    }  yield result
//  }
//
//  def getCTIInfo(customerId: String): Future[List[CTIInfo]] =  {
//    def query(collection: BSONCollection): Future[List[CTIInfo]] = {
//      collection.find(BSONDocument("event" -> "cti_service", "actor_id" -> customerId)).cursor[BSONDocument]().
//        collect[List]().map(list => list.map(doc => ctiInfoConverter(doc)))
//    }
//
//    QueryExecute(CUSTOMER_JOURNEY_DATABASE, CUSTOMER_JOURNEY_COLLECTION, query)
//  }


//  def requireICustomer(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[ICustomer] = {
//    implicit val reader = Macros.reader[ICustomer]
//    //implicit val writer = Macros.writer[ICustomer]
//    collection.find(BSONDocument(query)).requireOne[ICustomer]
//  }
//
//  def findList(collection: BSONCollection, query: BSONDocument): Future[List[BSONDocument]] = {
//    // only fetch the name field for the result documents
//    val projection = BSONDocument("author" -> 1)
//
//    collection.find(query/*, projection*/).cursor[BSONDocument]().
//      collect[List]() // get up to 25 documents
//  }


}
