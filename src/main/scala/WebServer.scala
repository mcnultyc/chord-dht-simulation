/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory.load
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


object WebServer{

  def toInt(string: String): Option[Int] ={
    try{
      Some(string.toInt)
    }catch {
      case e: Exception => None
    }
  }

  def main(args: Array[String]): Unit ={
    // Create root system for actors
    implicit val system: ActorSystem = akka.actor.ActorSystem("testing")
    // Needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    // Load the config file
    val config = load
    // Create server manager
    val manager = system.actorOf(Props[ServerManager], "servermanager")
    // Start the datacenter
    manager ! ServerManager.Start(config.getInt("sim1.num-servers"))
    // Set up scheduler for snapshots
    val delay = config.getInt("sim1.snapshot-interval")
    system.scheduler.scheduleWithFixedDelay(delay.seconds, delay.seconds, manager, ServerManager.WriteSnapshot)

    val xmlstyle = "<?xml-stylesheet href=\"#style\"\n   type=\"text/css\"?>"
    // Start running the web client
    WebClient.run()

    val route =
      concat(
        get {
          entity(as[String]){ movie =>{
            // Set timeout for lookup request
            implicit val timeout: Timeout = 10.seconds
            // Create future for lookup request
            val future = manager.ask(ServerManager.HttpLookUp(movie.toString()))
            // Create http route after lookup is ready
            onComplete(future) {
              // Match responses from the server manager
              case Success(ServerManager.FoundFile(filename, size)) => {
                // Create and send http response with information about request
                complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                  s"FILE FOUND!")))
              }
              case Success(ServerManager.FileNotFound(filename)) => {
                complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                  s"FILE NOT FOUND!")))
              }
              case Failure(ex) => complete((InternalServerError, s"ERROR: ${ex.getMessage}"))
            }
          }}
        },
        put {
          entity(as[String]) { input => {
            val tokens = input.split("\\|")
            if(tokens.size == 2){
              // Parse the movie name and size from the input
              val movie = tokens(0)
              // Check if second value is a valid integer
              val option = toInt(tokens(1).trim)
              option match{
                case Some(size) =>  {
                  // Send insert command to server manager
                  manager ! ServerManager.HttpInsert(movie, size)
                  complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                    s"FILE INSERTED!")))
                }
                case None =>  {
                  system.log.info(s"ERROR: INCORRECT SIZE! FILENAME: $movie, SIZE: ${tokens(1)}")
                  // Send parsing error message back to client
                  complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                    s"ERROR: INCORRECT SIZE! SIZE: ${tokens(1)}")))
                }
              }
            }
            else{
              system.log.info(s"ERROR: MISSING REQUIRED ARGUMENTS! INPUT: $input")
              // Send missing arguments error back to client
              complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                "ERROR: MISSING REQUIRED ARGUMENTS")))
            }
          }}
        },
        extractUnmatchedPath { anything => {
          get {
            complete("Send put requests to localhost:8080")
          }
        }
        }
      )

    val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(route, "localhost", 8080)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  }
}//end object WebServer
