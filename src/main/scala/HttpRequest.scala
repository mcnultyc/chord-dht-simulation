import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io.StdIn

class WebServer {
  def run  {
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val x = List("a","B")
    val xmlstyle = "<?xml-stylesheet href=\"#style\"\n   type=\"text/css\"?>"

    val route =get {
      concat(
        pathSingleSlash{
          complete(HttpEntity(ContentTypes.`text/xml(UTF-8)`,xmlstyle+ "<html><body>Hello world!</body></html>"))
        },
        pathPrefix("movies")
        {
          extractUnmatchedPath{ movieName=>
            val movie = movieName.dropChars(1)
            complete("Put function call to get movie here: "+movie)
          }
        }

      )
    }




    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}