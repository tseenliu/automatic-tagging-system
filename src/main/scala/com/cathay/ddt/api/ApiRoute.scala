package com.cathay.ddt.api

import org.slf4j.LoggerFactory
import spray.json._
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes._
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import com.cathay.ddt.tagging.schema.TagDictionary
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext

trait ApiRoute {

//  import com.cathay.ddt.tagging.schema.ComposeTDProtocol._


  implicit val system: ActorSystem
  implicit val materializer: Materializer
  implicit val ec: ExecutionContext

  val log = LoggerFactory.getLogger(this.getClass)


//  MongoConnector.getTDCollection.flatMap(tagColl => MongoUtils.findDictionaries(tagColl, query)).map { docList =>
//    docList.foreach(TD => tagManager ! Cmd(Load(TD)))
//  }

  import com.cathay.ddt.tagging.schema.TDProtocol._

  val route =
    pathPrefix("tags"){
      (post & entity(as[TagDictionary])) { td =>
        onSuccess(MongoConnector.getTDCollection.flatMap( coll => MongoUtils.insert(coll, td) )) {
          case true =>
            complete(StatusCodes.OK, JsObject(
              "message" -> JsString("Command deliver successfully.")
            ))
          case false =>
            complete(StatusCodes.BadRequest, JsObject(
              "message" -> JsString("Command deliver failed..")
            ))
        }

//        complete {
//          MongoConnector.getTDCollection.flatMap( coll => MongoUtils.insert(coll, td) ) map {
//            case true => Created -> Map("tag_id" -> td.tag_id).toJson
//          }
//        }
      } ~
        (get) {
          complete {
            val tmp = MongoConnector.getTDCollection.flatMap( coll => MongoUtils.findDictionaries(coll, BSONDocument()) )
            OK -> tmp
//            map { ts =>
//              OK -> ts
//            }
          }
        }


    }


}
