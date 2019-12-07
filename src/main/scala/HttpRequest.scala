import akka.actor.ActorSystem
import akka.http.javadsl.server.PathMatcher1
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.PathMatcher
import akka.parboiled2.RuleTrace.StringMatch
import akka.stream.ActorMaterializer

import scala.io.StdIn




  object WebServer {
    def main(args: Array[String]) {

      implicit val system = ActorSystem("my-system")
      implicit val materializer = ActorMaterializer()
      // needed for the future flatMap/onComplete in the end
      implicit val executionContext = system.dispatcher


      val route =
            path(Map("hello"->"hello","bye"->"bye")){x=>{
              complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say "+x+" to akka-http</h1>"))
            }

            }




      val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
      println("Server online at http://localhost:8080/\nPress RETURN to stop...")
      StdIn.readLine() // let it run until user presses return
      bindingFuture
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ => system.terminate()) // and shutdown when done
    }
  }

