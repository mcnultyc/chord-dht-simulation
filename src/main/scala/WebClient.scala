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
import akka.http.scaladsl.unmarshalling.Unmarshal

import scala.concurrent.Future
import scala.util.Random.shuffle
import scala.io.Source
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream._

import scala.annotation.tailrec


object WebClient {

  def run(): Unit ={
    implicit val system = ActorSystem()
    implicit val executionContext = system.dispatcher

    def sendGetRequests(files: List[String]): Unit = {
      // Select a file at random for get request
      val file = shuffle(files).head
      val request =
        HttpRequest(
          method = HttpMethods.GET,
          uri = "http://localhost:8080/",
          entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, file)
        )
      val future: Future[HttpResponse] = Http().singleRequest(request)
      future.onComplete {
        case Success(response) => {
          // Unmarshal response entity from http response
          Unmarshal(response.entity).to[String].onComplete {
            case Success(data) => {
              system.log.info(s"HTTP GET [$file]: $data")
              sendGetRequests(files)
            }
            case Failure(exception) => sys.error(exception.getMessage)
          }
        }
        case Failure(exception) => sys.error(exception.getMessage)
      }
    }
    
    system.log.info("STARTING WEB CLIENT")

    val input = Source.fromFile("src/main/resources/movies.txt")
    val lines = input.getLines().toList

    system.log.info("SENDING PUT REQUESTS")

    // Send put requests for each line in the file
    lines.foreach(line => {
      val request =
        HttpRequest(
          method = HttpMethods.PUT,
          uri = "http://localhost:8080/",
          entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, line) // Send entire line
        )
       val future: Future[HttpResponse] = Http().singleRequest(request)
       future.onComplete{
         case Success(response) => {
           // Unmarshal response entity from http response
            Unmarshal(response.entity).to[String].onComplete{
              // Log response from web servers
              case Success(data) => system.log.info(s"HTTP PUT [$line]: $data")
              case Failure(exception) => sys.error(exception.getMessage)
            }
         }
         case Failure(exception) => sys.error(exception.getMessage)
       }
    })
    
    system.log.info("SENDING GET REQUESTS")
    val files = lines.map(line => line.split("\\|")(0))
    sendGetRequests(files)
  }
}
