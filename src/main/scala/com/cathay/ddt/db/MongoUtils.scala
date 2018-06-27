package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.{DynamicCD, QueryCD, CustomerDictionary}
import org.slf4j.LoggerFactory
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.{BSONDocument, Macros}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by Tse-En on 2017/12/12.
  */
object MongoUtils extends TagDictionaryExtension {

  val log = LoggerFactory.getLogger(this.getClass)

  /* Write Documents */
  def insertDoc(coll: BSONCollection, doc: BSONDocument): Future[Boolean] = {
    val writeRes: Future[WriteResult] = coll.insert(doc)

    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(result => result.ok)
    //writeRes.map(_ => {}) // in this example, do nothing with the success
  }

  // For insert test
  def insert(coll: BSONCollection, td: CustomerDictionary): Future[Boolean] = {
    implicit val reader = Macros.reader[CustomerDictionary]
    implicit val writer = Macros.writer[CustomerDictionary]
    val writeRes: Future[WriteResult] = coll.insert(td)
    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(_.ok)
  }

  def insert(coll: BSONCollection, td: DynamicCD): Future[Boolean] = {
    implicit val dynamicTDHandler = Macros.handler[DynamicCD]
    val writeRes: Future[WriteResult] = coll.insert(td)
    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(_.ok)
  }

  def update(coll: BSONCollection, selector: BSONDocument, modifier: DynamicCD): Future[Boolean] = {
    // get a future update
    implicit val dynamicTDHandler = Macros.handler[DynamicCD]
    val futureUpdate = coll.update(selector, modifier)

    futureUpdate.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully updated document with result: $writeResult")
    }
    futureUpdate.map(result => result.ok)
  }

  def updateFind(collection: BSONCollection, selector: BSONDocument, modifier: CustomerDictionary): Future[Option[CustomerDictionary]] = {
    import collection.BatchCommands.FindAndModifyCommand.FindAndModifyResult

    val result: Future[FindAndModifyResult] = collection.findAndUpdate(
      selector, modifier, fetchNewObject = true, upsert = true)

    result.map(x => x.result[CustomerDictionary])
  }

  /* Remove Documents */
  def remove(coll: BSONCollection, selector: BSONDocument): Future[Boolean] = {
    val futureRemove = coll.remove(selector /*, firstMatchOnly = true*/)

    futureRemove.onComplete { // callback
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully removed document with result: $writeResult")
    }
    futureRemove.map(_.ok)
  }

  /* Find Documents */
  def findDictionaries(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[CustomerDictionary]] = {
//    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).cursor[CustomerDictionary]().collect[List]()
  }

  def findDictionaries(collection: BSONCollection, query: QueryCD)(implicit ec: ExecutionContext): Future[List[CustomerDictionary]] = {
    //    implicit val reader = Macros.reader[TagDictionary]
    implicit val queryTDHandler = Macros.handler[QueryCD]
    collection.find(query).cursor[CustomerDictionary]().collect[List]()
  }

  def findOneDictionary(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[CustomerDictionary] = {
//    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).requireOne[CustomerDictionary]
  }

}
