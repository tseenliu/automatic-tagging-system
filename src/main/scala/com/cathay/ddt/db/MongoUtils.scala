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
object MongoUtils extends TagDictionaryExtension {

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
  def findDictionaries(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[TagDictionary]] = {
//    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).cursor[TagDictionary]().collect[List]()
  }

  def findOneDictionary(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[TagDictionary] = {
//    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).requireOne[TagDictionary]
  }

  def updateFind(collection: BSONCollection, selector: BSONDocument, modifier: TagDictionary): Future[Option[TagDictionary]] = {
    import collection.BatchCommands.FindAndModifyCommand.FindAndModifyResult

    val result: Future[FindAndModifyResult] = collection.findAndUpdate(
      selector, modifier, fetchNewObject = true, upsert = true)

    result.map(x => x.result[TagDictionary])
  }

}
