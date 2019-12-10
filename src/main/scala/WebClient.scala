/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import scala.concurrent.Future
import scala.util.Random.shuffle
import scala.io.Source
import scala.util.{Failure, Success}

object WebClient {

  def run(): Unit ={
    implicit val system = ActorSystem()
    implicit val executionContext = system.dispatcher

    system.log.info("STARTING WEB CLIENT")

    val input = Source.fromFile("src/main/resources/movies.txt")
    val lines = input.getLines().toList

    system.log.info("SENDING PUT REQUESTS")

    lines.foreach(line => {
      val request =
        HttpRequest(
          method = HttpMethods.PUT,
          uri = "http://localhost:8080/",
          entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, line)
        )
       val future: Future[HttpResponse] = Http().singleRequest(request)
       future.onComplete{
         case Success(_) => system.log.info(s"HTTP PUT [$line] REQUEST SUCCEEDED")
         case Failure(exception) => sys.error(exception.getMessage)
       }
    })

    system.log.info("SENDING GET REQUESTS")

    val request =
      HttpRequest(
        method = HttpMethods.GET,
        uri = "http://localhost:8080/",
        entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, "testing")
      )

    system.log.info("messages all sent")
    /*
    val future: Future[HttpResponse] = Http().singleRequest(request)
    future.onComplete{
      case Success(_) => system.log.info("HTTP GET REQUEST SUCCEEDED")
      case Failure(exception) => sys.error(exception.getMessage)
    }
     */

    /*
    val files = lines.map(line => line.split("\\|")(0))

    (0 to 2000).foreach( _ =>{
      val file = shuffle(files).head
      val request =
        HttpRequest(
          method = HttpMethods.GET,
          uri = "http://localhost:8080/",
          entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, file)
        )
      Http().singleRequest(request)
    })

     */

  }
}
