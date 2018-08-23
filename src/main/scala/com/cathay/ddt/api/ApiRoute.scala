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
import com.cathay.ddt.ats.SegmentManager._
import com.cathay.ddt.tagging.schema.{DynamicSD, QuerySD, SegmentDictionary}
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

  implicit def convertTD(dtd: DynamicSD): SegmentDictionary = {
    SegmentDictionary(
      dtd.segment_id.get,
      dtd.segment_type.get,
      dtd.segment_name.get,
      dtd.sql.get,
      dtd.update_frequency.get,
      dtd.detail.get,
      dtd.description.get,
      dtd.create_time.get,
      dtd.update_time.get,
      dtd.disable_flag,
      dtd.creator.get,
      dtd.is_focus.get,
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
          "message" -> JsString(s"SegmentID is not exist.")
        ))
      }
  }

  val route = handleExceptions(myExceptionHandler) {
    val segmentManagerSelection: ActorSelection =
      system.actorSelection(s"akka.tcp://segment@$tmHost:$tmPort/user/segment-manager")

    pathPrefix("segments") {
      import com.cathay.ddt.tagging.protocal.SDProtocol._
      import com.cathay.ddt.tagging.protocal.DynamicSDProtocol._
      pathEnd {
        // write operation
        (post & entity(as[DynamicSD])) { td =>
          onSuccess(MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.insert(coll, td))) {
            case true =>
              log.info(s"Request to insert SegmentID[${td.actorID}].\n${convertTD(td).toJson.prettyPrint}")
              segmentManagerSelection ! Cmd(Load(convertTD(td)))
              Thread.sleep(500)
              segmentManagerSelection ! Cmd(ShowState)
              complete(StatusCodes.OK, JsObject(
                "message" -> JsString(s"SegmentID[${td.actorID}] insert successfully.")
              ))
            case false =>
              log.error(s"Request to insert SegmentID[${td.actorID}].")
              complete(StatusCodes.BadRequest, JsObject(
                "message" -> JsString(s"SegmentID[${td.actorID}] insert failed.")
              ))
          }
        }

      } ~
      // update operation
      path(Segment) { id =>
        put {
          entity(as[DynamicSD]) { td =>
            val query = BSONDocument("segment_id" -> id)
            onSuccess(MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.update(coll, query, td))) {
              case true =>
                log.info(s"Request to update SegmentID[${td.actorID}].\n${convertTD(td).toJson.prettyPrint}")
                segmentManagerSelection ! Cmd(Update(convertTD(td)))
                Thread.sleep(500)
                segmentManagerSelection ! Cmd(ShowState)
                complete(StatusCodes.OK, JsObject(
                  "message" -> JsString(s"SegmentID[${td.actorID}] update successfully.")
                ))
              case false =>
                log.error(s"Request to update SegmentID[${td.actorID}].")
                complete(StatusCodes.BadRequest, JsObject(
                  "message" -> JsString(s"SegmentID[${td.actorID}] update failed.")
                ))
            }
          }
        }

      } ~
        // find operation
        path("search") {
          import com.cathay.ddt.tagging.protocal.QuerySDProtocol._
          (post & entity(as[QuerySD])) { dtd =>
            complete {
              log.info(s"Request to search $dtd.")
              OK -> MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, dtd))
            }
          }

        } ~
        (get & path(Segment)) { id =>
          complete {
            log.info(s"Request to find SegmentID[$id].")
            val query = BSONDocument("segment_id" -> id)
            val TD = MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))
            OK -> TD
          }

        } ~
        // remove operation
        (delete & path(Segment)) { id =>
          val query = BSONDocument("segment_id" -> id)
          onSuccess(MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))){
            case m: SegmentDictionary =>
              onSuccess(MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.remove(coll, BSONDocument("segment_id" -> id)))) {
                case true =>
                  log.info(s"Request to remove SegmentID[$id].")
                  segmentManagerSelection ! Cmd(Remove(id))
                  Thread.sleep(500)
                  segmentManagerSelection ! Cmd(ShowState)
                  complete(StatusCodes.OK, JsObject(
                    "message" -> JsString(s"SegmentID[${id}] remove successfully.")
                  ))
                case false =>
                  log.error(s"Request to remove SegmentID[$id].")
                  complete(StatusCodes.BadRequest, JsObject(
                    "message" -> JsString(s"SegmentID[${id}] remove failed.")
                  ))
              }
            case _ =>
              log.error(s"Request to remove SegmentID[$id].")
              complete(StatusCodes.BadRequest, JsObject(
                "message" -> JsString(s"SegmentID[${id}] remove failed.")
              ))
          }
        } ~
        // find all operation
        pathEnd {
          (get) {
            complete {
              import com.cathay.ddt.tagging.protocal.DynamicSDProtocol._
              log.info(s"Request to get all segments.")
              val ListTD = MongoConnector.getCUSDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, BSONDocument()))
              OK -> ListTD
            }
          }
        }
    } ~
      pathPrefix("segments_history") {
        import com.cathay.ddt.tagging.protocal.SDProtocol._
        import com.cathay.ddt.tagging.protocal.DynamicSDProtocol._
        pathEnd {
          // write operation
          (post & entity(as[DynamicSD])) { td =>
            onSuccess(MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.insert(coll, td))) {
              case true =>
                log.info(s"Request to insert SegmentID[${td.actorID}].\n${convertTD(td).toJson.prettyPrint}")
                complete(StatusCodes.OK, JsObject(
                  "message" -> JsString(s"SegmentID[${td.actorID}] insert successfully.")
                ))
              case false =>
                log.error(s"Request to insert SegmentID[${td.actorID}].")
                complete(StatusCodes.BadRequest, JsObject(
                  "message" -> JsString(s"SegmentID[${td.actorID}] insert failed.")
                ))
            }
          }

        } ~
          // update operation
          path(Segment) { id =>
            put {
              entity(as[DynamicSD]) { td =>
                val query = BSONDocument("segment_id" -> id)
                onSuccess(MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.update(coll, query, td))) {
                  case true =>
                    log.info(s"Request to update SegmentID[${td.actorID}]\n${convertTD(td).toJson.prettyPrint}.")
                    complete(StatusCodes.OK, JsObject(
                      "message" -> JsString(s"SegmentID[${td.actorID}] update successfully.")
                    ))
                  case false =>
                    log.error(s"Request to update SegmentID[${td.actorID}].")
                    complete(StatusCodes.BadRequest, JsObject(
                      "message" -> JsString(s"SegmentID[${td.actorID}] update failed.")
                    ))
                }
              }
            }

          } ~
          // find operation
          path("search") {
            import com.cathay.ddt.tagging.protocal.QuerySDProtocol._
            (post & entity(as[QuerySD])) { dtd =>
              complete {
                log.info(s"Request to search $dtd.")
                OK -> MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, dtd))
              }
            }

          } ~
          (get & path(Segment)) { id =>
            complete {
              log.info(s"Request to find SegmentID[$id].")
              val query = BSONDocument("segment_id" -> id)
              val TD = MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))
              OK -> TD
            }

          } ~
          // remove operation
          (delete & path(Segment)) { id =>
            val query = BSONDocument("segment_id" -> id)
            onSuccess(MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.findOneDictionary(coll, query))){
              case m: SegmentDictionary =>
                onSuccess(MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.remove(coll, BSONDocument("segment_id" -> id)))) {
                  case true =>
                    log.info(s"Request to remove SegmentID[$id].")
                    complete(StatusCodes.OK, JsObject(
                      "message" -> JsString(s"SegmentID[${id}] remove successfully.")
                    ))
                  case false =>
                    log.error(s"Request to remove SegmentID[$id].")
                    complete(StatusCodes.BadRequest, JsObject(
                      "message" -> JsString(s"SegmentID[${id}] remove failed.")
                    ))
                }
              case _ =>
                log.error(s"Request to remove SegmentID[$id].")
                complete(StatusCodes.BadRequest, JsObject(
                  "message" -> JsString(s"SegmentID[${id}] remove failed.")
                ))
            }
          } ~
          // find all operation
          pathEnd {
            (get) {
              complete {
                import com.cathay.ddt.tagging.protocal.DynamicSDProtocol._
                log.info(s"Request to get all segments.")
                val ListTD = MongoConnector.getHCUSDCollection.flatMap(coll => MongoUtils.findDictionaries(coll, BSONDocument()))
                OK -> ListTD
              }
            }
          }
      }

  }
}
