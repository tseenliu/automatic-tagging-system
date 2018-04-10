package com.cathay.ddt.api

import org.slf4j.LoggerFactory
import spray.json._
import akka.actor.{ActorSelection, ActorSystem}
import akka.stream.Materializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes._
import com.cathay.ddt.db.{MongoConnector, MongoUtils}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.ExceptionHandler
import com.cathay.ddt.ats.TagManager._
import com.cathay.ddt.tagging.schema.{DynamicTD, QueryTD, TagDictionary}
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext

trait ApiRoute {

  implicit val system: ActorSystem
  implicit val materializer: Materializer
  implicit val ec: ExecutionContext

  implicit def convertTD(dtd: DynamicTD): TagDictionary = {
    TagDictionary(
      dtd.tag_id.get,
      dtd.source_type.get,
      dtd.source_item.get,
      dtd.tag_type.get,
      dtd.tag_name.get,
      dtd.sql.get,
      dtd.update_frequency.get,
      dtd.started,
      dtd.traced,
      dtd.description.get,
      dtd.create_time.get,
      dtd.update_time.get,
      dtd.disable_flag,
      dtd.score_method.get,
      dtd.attribute.get,
      dtd.creator.get,
      dtd.is_focus.get,
      dtd.system_name.get
    )
  }

  // logging
  val log = LoggerFactory.getLogger(this.getClass)

  val myExceptionHandler = ExceptionHandler {
    case _: reactivemongo.api.Cursor.NoSuchResultException.type =>
      extractUri { uri =>
        println(s"Request to $uri could not be handled normally")
//        complete(HttpResponse(InternalServerError, entity = "Bad numbers, bad result!!!"))
        complete(StatusCodes.BadRequest, JsObject(
          "message" -> JsString(s"tagID is not exist.")
        ))
      }
  }

  val route = handleExceptions(myExceptionHandler) {

    val tagManagerSelection: ActorSelection =
      system.actorSelection("akka.tcp://tag@127.0.0.1:2551/user/tag-manager")

    pathPrefix("tags") {
      import com.cathay.ddt.tagging.protocal.TDProtocol._
      import com.cathay.ddt.tagging.protocal.DynamicTDProtocol._
      pathEnd {
        // write operation
        (post & entity(as[DynamicTD])) { td =>
          //        complete {
          //          MongoConnector.getTDCollection.flatMap( coll => MongoUtils.insert(coll, td) ) map {
          //            case true => Created -> Map("tag_id" -> td.tag_id).toJson
          //          }
          //        }
          onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.insert(coll, td))) {
            case true =>
              tagManagerSelection ! Cmd(Load(convertTD(td)))
              Thread.sleep(500)
              tagManagerSelection ! Cmd(ShowState)
              complete(StatusCodes.OK, JsObject(
                "message" -> JsString(s"tagID[${td.tag_id}] insert successfully.")
              ))
            case false =>
              complete(StatusCodes.BadRequest, JsObject(
                "message" -> JsString("tagID[${td.tag_id}] insert failed.")
              ))
          }
        }

      } ~
      // update operation
      path(Segment) { id =>
        put {
          entity(as[DynamicTD]) { td =>
//              complete {
//                val query = BSONDocument("tag_id" -> id)
//                val TD = MongoConnector.getTDCollection.flatMap(coll => MongoUtils.updateFind(coll, query, td))
//                OK -> TD
//              }
            val query = BSONDocument("tag_id" -> id)
            onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.update(coll, query, td))) {
              case true =>
                tagManagerSelection ! Cmd(Update(convertTD(td)))
                Thread.sleep(500)
                tagManagerSelection ! Cmd(ShowState)
                complete(StatusCodes.OK, JsObject(
                  "message" -> JsString(s"tagID[${td.tag_id}] update successfully.")
                ))
              case false =>
                complete(StatusCodes.BadRequest, JsObject(
                  "message" -> JsString("tagID[${td.tag_id}] update failed.")
                ))
            }
          }
        }

      } ~
        // find operation
        path("search") {
          import com.cathay.ddt.tagging.protocal.QueryTDProtocol._
          (post & entity(as[QueryTD])) { dtd =>
//            var bArr = ArrayBuffer[BSONDocument]()
//            var query = BSONDocument()
//            if(dtd.source_type.isDefined) {
//              query ++= BSONDocument("source_type" -> dtd.source_type.get)
//            }else if(dtd.source_item.isDefined) {
//              query ++= BSONDocument("source_item" -> dtd.source_item.get)
//            }

//            if (td.tag_type.isDefined) {
//              val typeList = td.tag_type.get
//              for (i <- td.tag_type.get.indices) {
//                bArr += BSONDocument("type_L1" -> typeList(i).type_L1, "type_L2" -> typeList(i).type_L2)
//              }
//            }
//            query = BSONDocument(
//              "source_type" -> td.source_type.get,
//              "source_item" -> td.source_item.get,
//              "tag_type" -> BSONArray(bArr),
//              "tag_name" -> td.tag_name.get,
//              "update_frequency" -> td.update_frequency.get,
//              "started" -> td.started.get,
//              "traced" -> td.traced.get,
//              "score_method" -> td.score_method.get,
//              "attribute" -> td.attribute.get,
//              "system_name" -> td.system_name.get)
            complete {
              OK -> MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, dtd))
            }
          }

        } ~
        (get & path(Segment)) { id =>
          complete {
            val query = BSONDocument("tag_id" -> id)
            val TD = MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))
            OK -> TD
          }

        } ~
        // remove operation
        (delete & path(Segment)) { id =>
          onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.remove(coll, BSONDocument("tag_id" -> id)))) {
            case true =>
              tagManagerSelection ! Cmd(Remove(id))
              Thread.sleep(500)
              tagManagerSelection ! Cmd(ShowState)
              complete(StatusCodes.OK, JsObject(
                "message" -> JsString(s"tagID[${id}] remove successfully.")
              ))
            case false =>
              complete(StatusCodes.BadRequest, JsObject(
                "message" -> JsString(s"tagID[${id}] remove failed.")
              ))
          }

        } ~
        // find all operation
        (get) {
          complete {
            import com.cathay.ddt.tagging.protocal.DynamicTDProtocol._
            val ListTD = MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, BSONDocument()))
            OK -> ListTD
          }
        }
    }

  }
}
