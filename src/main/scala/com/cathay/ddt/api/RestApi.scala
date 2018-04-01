package com.cathay.ddt.api

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.http.scaladsl.Http

object RestApi extends App with ApiRoute {

  override implicit val system = ActorSystem("mongo-rest-api")

  override implicit val materializer = ActorMaterializer()

  override implicit val ec = system.dispatcher

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  Console.readLine()

  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())

}
