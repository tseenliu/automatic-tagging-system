package com.cathay.ddt.app

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.cathay.ddt.api.ApiRoute
import com.cathay.ddt.utils.EnvLoader

object BootRestApi extends App with ApiRoute with EnvLoader {

  val config = getConfig("api")
  val host = config.getString("api.host")
  val port = config.getInt("api.port")

  override implicit val system = ActorSystem("mongo-rest-api", config.getConfig("TagApi"))

  override implicit val materializer = ActorMaterializer()

  override implicit val ec = system.dispatcher

  val bindingFuture = Http().bindAndHandle(route, host, port)

  println(s"Server online at http://$host:$port/\nWelcome to use ATS rest api!")
//  Console.readLine()

//  bindingFuture
//    .flatMap(_.unbind())
//    .onComplete(_ => system.terminate())

}
