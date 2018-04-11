package com.cathay.ddt.api

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import com.cathay.ddt.utils.EnvLoader

object RestApi extends App with ApiRoute with EnvLoader {

  override implicit val system = ActorSystem("mongo-rest-api", config)

  override implicit val materializer = ActorMaterializer()

  override implicit val ec = system.dispatcher

  val config = getConfig("api")
  val bindingFuture = Http().bindAndHandle(route, config.getString("api.host"), config.getInt("api.port"))

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  Console.readLine()

  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())

}
