package com.cathay.ddt.utils

import spray.json._

class YarnJsonProtocol {

//  case class YarnMetrics(clusterMetrics: ClusterMetrics)
//  case class ClusterMetrics(
//                             appsSubmitted: Int,
//                             appsCompleted: Int,
//                             appsPending: Int,
//                             appsRunning: Int,
//                             appsFailed: Int,
//                             appsKilled: Int,
//                             reservedMB: Int,
//                             availableMB: Int,
//                             allocatedMB: Int,
//                             reservedVirtualCores: Int,
//                             availableVirtualCores: Int,
//                             allocatedVirtualCores: Int,
//                             containersAllocated: Int,
//                             containersReserved: Int,
//                             containersPending: Int,
//                             totalMB: Int,
//                             totalVirtualCores: Int,
//                             totalNodes: Int,
//                             lostNodes: Int,
//                             unhealthyNodes: Int,
//                             decommissioningNodes: Int,
//                             decommissionedNodes: Int,
//                             rebootedNodes: Int,
//                             activeNodes: Int,
//                             shutdownNodes: Int)
//
//
//  object InfoProtocol extends DefaultJsonProtocol {
//    implicit val infoFormat = jsonFormat25(ClusterMetrics)
//  }
//
//  import InfoProtocol._
//
//
//  object MyJsonProtocol extends DefaultJsonProtocol {
//
//    implicit object ColorJsonFormat extends RootJsonFormat[Color] {
//      def write(c: Color) = JsObject(
//        "name" -> JsString(c.name),
//        "red" -> JsNumber(c.red),
//        "green" -> JsNumber(c.green),
//        "blue" -> JsNumber(c.blue),
//        "info" ->  c.info.toJson
//      )
//      def read(value: JsValue): Color = {
//        val jso = value.asJsObject
//        val info = jso.fields("info").convertTo[Info]
//        value.asJsObject.getFields("name", "red", "green", "blue") match {
//          case Seq(JsString(name), JsNumber(red), JsNumber(green), JsNumber(blue)) =>
//            Color(name, red.toInt, green.toInt, blue.toInt, info)
//          case _ => throw DeserializationException("Color expected")
//        }
//      }
//    }
//  }
//
//  import MyJsonProtocol._
//
//  val json = Color("CadetBlue", 95, 158, 160, Info(1,3)).toJson
//  //val color = json.convertTo[Color]
//  println(json.prettyPrint)
//  //println(color)

}
