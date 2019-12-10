/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import java.io.File

import akka.actor.{ActorSystem, Props}
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

  // Helper function to try parsing an integer
  def toInt(string: String): Option[Int] ={
    try{
      Some(string.toInt)
    }catch {
      case e: Exception => None
    }
  }

  // Helper function to try parsing a boolean
  def toBool(string: String): Option[Boolean] ={
    try{
      Some(string.toBoolean)
    }catch{
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
    // Setup configuration
    var numServers = config.getInt("sim1.num-servers")
    // Flag to enable usage of the finger table
    var enableTable = config.getBoolean("sim1.enable-table")
    // Interval between each snapshot
    var delay = config.getInt("sim1.snapshot-interval")
    // Attempt to parse arguments from command line
    if(args.length >= 1){
      // Try to parse the number of servers argument as integer
      val numServersOption = toInt(args(0))
      if(numServersOption.isDefined){
        // Update servers to parsed integer
        numServers = numServersOption.get
      }
    }
    if(args.length >= 2){
      // Try to parse the enable table flag as a boolean
      val enableTableOptions = toBool(args(1))
      if(enableTableOptions.isDefined){
        // Update enable table flag to parsed boolean
        enableTable = enableTableOptions.get
      }
    }
    if(args.length >= 3) {
      // Try to parse the snapshot interval as integer
      val delayOption = toInt(args(2))
      if (delayOption.isDefined) {
        // Update snapshot interval to parsed integer
        delay = delayOption.get
      }
    }
    // Start the datacenter
    manager ! ServerManager.Start(numServers, enableTable)
    // Set up scheduler for snapshots
    system.scheduler.scheduleWithFixedDelay(delay.seconds, delay.seconds, manager, ServerManager.WriteSnapshot)

    val xmlstyle = "<?xml-stylesheet href=\"#style\"\n   type=\"text/css\"?>"
    // Start running the web client
    WebClient.run()

    val route =
      concat(
        path("snapshot"){
          get{
            // Get filenames from the snapshots directory
            val dir = new File("snapshots")
            dir.listFiles.filter(_.isFile).toList.toString()
            // Get most recent snapshot from the snapshots directory
            val file = (dir.listFiles.filter(_.isFile).toList.last).toString
            getFromDirectory(file)
          }
        },
        path("log"){
          get{
            // Get filenames from the logs directory
            val dir = new File("logs")
            dir.listFiles.filter(_.isFile).toList.toString()
            // Get the most log file from the logs directory
            val file = (dir.listFiles.filter(_.isFile).toList.last).toString
            getFromDirectory(file)
          }
        },
        get {
          entity(as[String]){ movie =>{
            // Set timeout for lookup request
            implicit val timeout: Timeout = 10.seconds
            // Create future for lookup request
            val future = manager.ask(ServerManager.HttpLookUp(movie))
            // Create http route after lookup is ready
            onComplete(future) {
              // Match responses from the server manager
              case Success(ServerManager.FoundFile(filename, size)) =>
                // Create and send http response with information about request
                complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                  "FILE FOUND!")))
              case Success(ServerManager.FileNotFound(filename)) =>
                complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                  "FILE NOT FOUND!")))
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
                case Some(size) =>
                  // Send insert command to server manager
                  manager ! ServerManager.HttpInsert(movie, size)
                  complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                    "FILE INSERTED!")))
                case None =>
                  system.log.info(s"ERROR: INCORRECT SIZE! FILENAME: $movie, SIZE: ${tokens(1)}")
                  // Send parsing error message back to client
                  complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                    s"ERROR: INCORRECT SIZE! SIZE: ${tokens(1)}")))
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

    val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(route, "0.0.0.0", 8080)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  }
}//end object WebServer
