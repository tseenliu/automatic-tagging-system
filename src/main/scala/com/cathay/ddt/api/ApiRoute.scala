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
import com.cathay.ddt.utils.EnvLoader
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext

trait ApiRoute extends EnvLoader{

  val tmConfig = getConfig("ats")
  val tmHost = config.getString("ats.TagManager.akka.remote.netty.tcp.hostname")
  val tmPort = config.getInt("ats.TagManager.akka.remote.netty.tcp.port")

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
      dtd.system_name.get,
      dtd.tickets.get
    )
  }

  // logging
  val log = LoggerFactory.getLogger(this.getClass)

  val myExceptionHandler = ExceptionHandler {
    case _: reactivemongo.api.Cursor.NoSuchResultException.type =>
      extractUri { uri =>
        log.warn(s"Request to $uri could not be handled normally")
        complete(StatusCodes.BadRequest, JsObject(
          "message" -> JsString(s"tagID is not exist.")
        ))
      }
  }

  val route = handleExceptions(myExceptionHandler) {
    val tagManagerSelection: ActorSelection =
      system.actorSelection(s"akka.tcp://tag@$tmHost:$tmPort/user/tag-manager")

    pathPrefix("tags") {
      import com.cathay.ddt.tagging.protocal.TDProtocol._
      import com.cathay.ddt.tagging.protocal.DynamicTDProtocol._
      pathEnd {
        // write operation
        (post & entity(as[DynamicTD])) { td =>
          onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.insert(coll, td))) {
            case true =>
              log.info(s"Request to insert tagID[${td.tag_id}].")
              tagManagerSelection ! Cmd(Load(convertTD(td)))
              Thread.sleep(500)
              tagManagerSelection ! Cmd(ShowState)
              complete(StatusCodes.OK, JsObject(
                "message" -> JsString(s"tagID[${td.tag_id}] insert successfully.")
              ))
            case false =>
              log.error(s"Request to insert tagID[${td.tag_id}].")
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
            val query = BSONDocument("tag_id" -> id)
            onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.update(coll, query, td))) {
              case true =>
                log.info(s"Request to update tagID[${td.tag_id}].")
                tagManagerSelection ! Cmd(Update(convertTD(td)))
                Thread.sleep(500)
                tagManagerSelection ! Cmd(ShowState)
                complete(StatusCodes.OK, JsObject(
                  "message" -> JsString(s"tagID[${td.tag_id}] update successfully.")
                ))
              case false =>
                log.error(s"Request to update tagID[${td.tag_id}].")
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
            complete {
              log.info(s"Request to search $dtd.")
              OK -> MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, dtd))
            }
          }

        } ~
        (get & path(Segment)) { id =>
          complete {
            log.info(s"Request to find tagID[$id].")
            val query = BSONDocument("tag_id" -> id)
            val TD = MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))
            OK -> TD
          }

        } ~
        // remove operation
        (delete & path(Segment)) { id =>
          val query = BSONDocument("tag_id" -> id)
          onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))){
            case m: TagDictionary =>
              onSuccess(MongoConnector.getTDCollection.flatMap(coll => MongoUtils.remove(coll, BSONDocument("tag_id" -> id)))) {
                case true =>
                  log.info(s"Request to remove tagID[$id].")
                  tagManagerSelection ! Cmd(Remove(id))
                  Thread.sleep(500)
                  tagManagerSelection ! Cmd(ShowState)
                  complete(StatusCodes.OK, JsObject(
                    "message" -> JsString(s"tagID[${id}] remove successfully.")
                  ))
                case false =>
                  log.error(s"Request to remove tagID[$id].")
                  complete(StatusCodes.BadRequest, JsObject(
                    "message" -> JsString(s"tagID[${id}] remove failed.")
                  ))
              }
            case _ =>
              log.error(s"Request to remove tagID[$id].")
              complete(StatusCodes.BadRequest, JsObject(
                "message" -> JsString(s"tagID[${id}] remove failed.")
              ))
          }
        } ~
        // find all operation
        pathEnd {
          (get) {
            complete {
              import com.cathay.ddt.tagging.protocal.DynamicTDProtocol._
              log.info(s"Request to get tags.")
              val ListTD = MongoConnector.getTDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, BSONDocument()))
              OK -> ListTD
            }
          }
        }
    } ~
      pathPrefix("history") {
        import com.cathay.ddt.tagging.protocal.TDProtocol._
        import com.cathay.ddt.tagging.protocal.DynamicTDProtocol._
        pathEnd {
          // write operation
          (post & entity(as[DynamicTD])) { td =>
            onSuccess(MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.insert(coll, td))) {
              case true =>
                log.info(s"Request to insert tagID[${td.tag_id}].")
                tagManagerSelection ! Cmd(Load(convertTD(td)))
                complete(StatusCodes.OK, JsObject(
                  "message" -> JsString(s"tagID[${td.tag_id}] insert successfully.")
                ))
              case false =>
                log.error(s"Request to insert tagID[${td.tag_id}].")
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
                val query = BSONDocument("tag_id" -> id)
                onSuccess(MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.update(coll, query, td))) {
                  case true =>
                    log.info(s"Request to update tagID[${td.tag_id}].")
                    tagManagerSelection ! Cmd(Update(convertTD(td)))
                    complete(StatusCodes.OK, JsObject(
                      "message" -> JsString(s"tagID[${td.tag_id}] update successfully.")
                    ))
                  case false =>
                    log.error(s"Request to update tagID[${td.tag_id}].")
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
              complete {
                log.info(s"Request to search $dtd.")
                OK -> MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, dtd))
              }
            }

          } ~
          (get & path(Segment)) { id =>
            complete {
              log.info(s"Request to find tagID[$id].")
              val query = BSONDocument("tag_id" -> id)
              val TD = MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))
              OK -> TD
            }

          } ~
          // remove operation
          (delete & path(Segment)) { id =>
            val query = BSONDocument("tag_id" -> id)
            onSuccess(MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))){
              case m: TagDictionary =>
                onSuccess(MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.remove(coll, BSONDocument("tag_id" -> id)))) {
                  case true =>
                    log.info(s"Request to remove tagID[$id].")
                    tagManagerSelection ! Cmd(Remove(id))
                    complete(StatusCodes.OK, JsObject(
                      "message" -> JsString(s"tagID[${id}] remove successfully.")
                    ))
                  case false =>
                    log.error(s"Request to remove tagID[$id].")
                    complete(StatusCodes.BadRequest, JsObject(
                      "message" -> JsString(s"tagID[${id}] remove failed.")
                    ))
                }
              case _ =>
                log.error(s"Request to remove tagID[$id].")
                complete(StatusCodes.BadRequest, JsObject(
                  "message" -> JsString(s"tagID[${id}] remove failed.")
                ))
            }
          } ~
          // find all operation
          pathEnd {
            (get) {
              complete {
                import com.cathay.ddt.tagging.protocal.DynamicTDProtocol._
                log.info(s"Request to get tags.")
                val ListTD = MongoConnector.getHTDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, BSONDocument()))
                OK -> ListTD
              }
            }
          }
      }

  }
}
