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
import scala.util.{ Failure, Success }

object WebClient {

  def run(): Unit ={
    implicit val system = ActorSystem()
    implicit val executionContext = system.dispatcher

    val request =
    HttpRequest(
      method = HttpMethods.PUT,
      uri = "http://localhost:8080/",
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, "The Great Dictator (1940)|72")
    )

    Http().singleRequest(request)

  }
}
