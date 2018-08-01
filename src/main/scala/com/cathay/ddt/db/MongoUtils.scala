package com.cathay.ddt.db

import com.cathay.ddt.tagging.schema.{SegmentDictionary, DynamicSD, QuerySD, TagDictionary}
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
object MongoUtils extends DictionaryExtension {
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
  def insert(coll: BSONCollection, td: SegmentDictionary): Future[Boolean] = {
    implicit val reader = Macros.reader[SegmentDictionary]
    implicit val writer = Macros.writer[SegmentDictionary]
    val writeRes: Future[WriteResult] = coll.insert(td)
    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(_.ok)
  }

  def insert(coll: BSONCollection, td: DynamicSD): Future[Boolean] = {
    implicit val dynamicTDHandler = Macros.handler[DynamicSD]
    val writeRes: Future[WriteResult] = coll.insert(td)
    writeRes.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully inserted document with result: $writeResult")
    }
    writeRes.map(_.ok)
  }

  def update(coll: BSONCollection, selector: BSONDocument, modifier: DynamicSD): Future[Boolean] = {
    // get a future update
    implicit val dynamicTDHandler = Macros.handler[DynamicSD]
    val futureUpdate = coll.update(selector, modifier)

    futureUpdate.onComplete { // Dummy callbacks
      case Failure(e) => log.error(s"${e.printStackTrace().toString}")
      case Success(writeResult) => log.info(s"successfully updated document with result: $writeResult")
    }
    futureUpdate.map(result => result.ok)
  }

  def updateFind(collection: BSONCollection, selector: BSONDocument, modifier: SegmentDictionary): Future[Option[SegmentDictionary]] = {
    import collection.BatchCommands.FindAndModifyCommand.FindAndModifyResult

    val result: Future[FindAndModifyResult] = collection.findAndUpdate(
      selector, modifier, fetchNewObject = true, upsert = true)

    result.map(x => x.result[SegmentDictionary])
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
  def findDictionaries(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[SegmentDictionary]] = {
//    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).cursor[SegmentDictionary]().collect[List]()
  }

  def findTagDictionaries(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[TagDictionary]] = {
    collection.find(query).cursor[TagDictionary]().collect[List]()
  }

  def findDictionaries(collection: BSONCollection, query: QuerySD)(implicit ec: ExecutionContext): Future[List[SegmentDictionary]] = {
    //    implicit val reader = Macros.reader[TagDictionary]
    implicit val queryTDHandler = Macros.handler[QuerySD]
    collection.find(query).cursor[SegmentDictionary]().collect[List]()
  }

  def findOneDictionary(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[SegmentDictionary] = {
//    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).requireOne[SegmentDictionary]
  }

}
