package com.cathay.ddt.app.test

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.tagging.schema.TagDictionary
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.BSONDocument
import scala.concurrent.{ExecutionContext, Future}

object DynamicTimeTest extends App {


  def findDictionaries(collection: BSONCollection, query: BSONDocument)(implicit ec: ExecutionContext): Future[List[TagDictionary]] = {
    //    implicit val reader = Macros.reader[TagDictionary]
    collection.find(query).cursor[TagDictionary]().collect[List]()
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  MongoConnector.getTDCollection.flatMap(tagColl => MongoUtils.findDictionaries(tagColl, BSONDocument())).map { docList =>
    docList.foreach(println(_))
  }
}