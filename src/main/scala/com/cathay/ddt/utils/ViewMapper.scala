package com.cathay.ddt.utils

import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import com.cathay.ddt.tagging.schema.TagMessage
import com.cathay.ddt.tagging.schema.TagMessage.{Message, SimpleTagMessage}
import reactivemongo.bson.BSONDocument

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class ViewMapper {
  import ViewMapper._

  def initial(): Future[Any] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    MongoConnector.getTDCollection.flatMap(tagColl => MongoUtils.findTagDictionaries(tagColl, BSONDocument())).map { docList =>
      docList.foreach( td => addItem(td.update_frequency, td.tag_id, Some("tag_id")))
    }
  }

  def addItem(frequency: String, segement_id: String, partition: Option[String]) {
    sqlMList += ((segement_id, SimpleTagMessage(frequency, segement_id, partition)))
  }

  def getSqlMList: ListBuffer[(String, TagMessage.Message)] = sqlMList
}

object ViewMapper extends EnvLoader {
  private final val viewMapper = new ViewMapper
  def getViewMapper: ViewMapper = viewMapper

  val sqlMList = new ListBuffer[(String, Message)]()
}
