/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import Server.Lookup
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.NotUsed
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory.load
import java.io.{File, PrintWriter}
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.xml.bind.annotation.adapters.HexBinaryAdapter
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import scala.util.Random.shuffle
import scala.xml.{Elem, NodeBuffer, PrettyPrinter}


class MD5{
  private val hashAlgorithm = MessageDigest.getInstance("MD5")
  private val hexAdapter = new HexBinaryAdapter()
  def hash(value: String): BigInt ={
    // Hash value using MD5
    val bytes = hashAlgorithm.digest(value.getBytes())
    // Convert hash to hexadecimal
    val hex = hexAdapter.marshal(bytes)
    // Convert hexadecimal to unsigned integer
    BigInt(hex, 16)
  }
}

// Class to keep metadata for routing requests
class RouteMetadata{
  var hops: Int = 0
}

// Class to keep metadata for inserts and lookups
class FileMetadata(filename: String, size: Int){
  def getFilename: String ={
    filename
  }
  def getSize: Int ={
    size
  }
}

object Server{

  // Helper class to store length of routing paths
  final case class RouteMetadata(hops: Int)

  sealed trait Command
  // Commands to start and pause the system
  case object Start extends Command
  case object Pause extends Command

  // Command to update the stats for the server
  final case class UpdateStats(hops: Int) extends Command

  // Commands to update successors and handle routing requests
  final case class SetSuccessor(next: ActorRef[Server.Command], nextId: BigInt,
                                replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class FindSuccessor(replyTo: ActorRef[Server.Command], id: BigInt, index: Int, hops: Int) extends Command
  final case class FoundSuccessor(successor: ActorRef[Command], id: BigInt, index: Int, hops: Int) extends Command

  final case class SetPrev(prev: ActorRef[Server.Command], prevId: BigInt,
                           replyTo: ActorRef[ServerManager.Command]) extends Command

  // Commands to update server ids
  final case class SetId(id: BigInt, replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class GetId(replyTo: ActorRef[Command]) extends Command
  final case class RespondId(id: BigInt) extends Command
  // Commands to update table and respond updated table
  final case class UpdateTable(replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class UpdatedTable(table: List[(BigInt, ActorRef[Command])],
                                replyTo: ActorRef[ServerManager.Command]) extends Command
  // Commands to insert and lookup data
  final case class Insert(filename: String, size: Int, replyTo: ActorRef[ServerManager.Command]) extends Command

  final case class Lookup(filename: String, replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class GetFile(filename: String, replyTo: ActorRef[ServerManager.Command], hops: Int) extends Command

  //final case class FoundFile(data: FileMetadata) extends Command
  final case class FileNotFound(filename: String) extends Command

  // Command for snapshot
  final case class GetData(replyTo: ActorRef[ServerManager.Command]) extends Command

  // Largest value created by a 128 bit hash such as MD5
  val md5Max: BigInt = BigInt(1) << 128

  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Server(context))
}//end object Server

class Server(context: ActorContext[Server.Command])
  extends AbstractBehavior[Server.Command](context){
  import Server._
  // Actor reference to the next server in the chord ring
  private var next: ActorRef[Server.Command] = _
  // Cache id of next server in the chord ring
  private var nextId: BigInt = -1
  // Set previous node in chord ring
  private var prev: ActorRef[Server.Command] = _
  // Cache id of the previous server in the chord ring
  private var prevId: BigInt = -1
  // Finger table used for routing messages
  private var table: List[(BigInt, ActorRef[Command])] = _
  // Set id as hash of context username
  private var id: BigInt = -1
  // Store data keys in a set
  private val data = mutable.Map[String, FileMetadata]()
  // Store the number of total hops
  private var hops: BigInt = 0
  // Total number insert/lookup requests started at this node
  private var totalRequests: BigInt = 0

  // Ids for finger table entries, n+2^{k-1} for 1 <= k <= 128
  var tableIds: List[BigInt] = _

  /* Find the node in the finger table closest to the ID.
   */
  def closestPrecedingNode(id: BigInt): ActorRef[Command] ={
    var myRef = next
    if(table != null) {
      var bestRefIndex = md5Max;
      table.foreach { case (fingerId, ref) => {
        // Select node with highest key that can fit the id given
        val newRefIndexDiff =  id - fingerId
        if(newRefIndexDiff >= 0 && newRefIndexDiff < bestRefIndex) {
          bestRefIndex = newRefIndexDiff
          myRef = ref
        }
      }
      }
    }
    myRef // just use next to lookup nodes
  }

  def lookupFile(parent: ActorRef[Command], replyTo: ActorRef[ServerManager.Command], filename: String, size: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        val key = new MD5().hash(filename)
        // Find node that should have file
        parent ! FindSuccessor(context.self, key, -1, -1)

        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index, hops) =>
            // Update stats for node
            parent ! UpdateStats(hops)
            // Get file from selected node and forward reply
            successor ! GetFile(filename, replyTo, hops)
            Behaviors.stopped
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }


  def insertFile(parent: ActorRef[Command], replyTo: ActorRef[ServerManager.Command], filename: String, size: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        val key = new MD5().hash(filename)
        // Find node to insert file in
        parent ! FindSuccessor(context.self, key, -1, -1)
        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index, hops) =>
            context.log.info(s"FILE INSERTED! NAME: $filename, SIZE: $size, HOPS: $hops")
            // Update stats for node
            parent ! UpdateStats(hops)
            // Insert file in located node
            successor ! Insert(filename, size, replyTo)
            Behaviors.stopped
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  def updateTable(parent: ActorRef[Command], replyTo: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Counter for the number of responses
        var responses = 0
        // Request successor for each entry in finger table through immediate next
        var index = 0
        tableIds.foreach(id => {
          index += 1
          next ! FindSuccessor(context.self, id, index, -1)
        })
        val replies = mutable.ListBuffer[(ActorRef[Command], BigInt, Int)]()
        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index, hops) =>
            responses += 1
            replies += ((successor, id, index))
            // Check if all requests have been responded too
            if(responses == tableIds.size){
              // Sort by index
              val table = replies.sortBy(_._3).map{ case(server, id, _) => (id, server) }
              // Send updated table to be processed
              parent ! UpdatedTable(table.toList, replyTo)
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  /* Behavior for child session to find successor.
   */
  def findSuccessor(parent: ActorRef[Command], replyTo: ActorRef[Command], id: BigInt, index: Int, hops: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Case where we insert at first node
        if((prevId > this.id && id <= this.id) ||
           (prevId > this.id && id > prevId) ||
           (id > prevId && id <= this.id)){
          replyTo ! FoundSuccessor(parent, id, index, hops)
          Behaviors.stopped
        }
        // Case where we insert at next node
        else if((id > this.id && id <= nextId) ||
                (this.id > nextId && id <= nextId) ||
                (this.id > nextId && id > this.id)){
          replyTo ! FoundSuccessor(next, id, index, hops)
          Behaviors.stopped
        }
        else{
          val node = closestPrecedingNode(id)
          // Case where we route request
          node ! FindSuccessor(context.self, id, index, hops)
          Behaviors.receiveMessage{
            case FoundSuccessor(successor, id, index, hops) =>
              // Forward successor to actor that requested successor
              replyTo ! FoundSuccessor(successor,id, index, hops)
              // Stop child session
              Behaviors.stopped
            case _ => Behaviors.unhandled
          }
        }
      }.narrow[NotUsed]
  }

  def report(replyTo: ActorRef[ServerManager.Command]): Boolean ={
    if(next != null && nextId != -1 && prev != null && prevId != -1){
      replyTo ! ServerManager.ServerWarmedUp
      true
    }
    else{
      false
    }
  }

  override def onMessage(msg: Command): Behavior[Command] ={
    msg match{
      case FindSuccessor(ref, id, index, hops) =>
        // Create child session to handle successor request (concurrent)
        context.spawnAnonymous(findSuccessor(context.self, ref, id, index, hops+1))
        this
      case SetId(id, replyTo) =>
        this.id = id
        // Create table ids for server's finger table
        tableIds =
          (0 to 127).map(i => {
            // Set n = id + 2^i
            var n = this.id + (BigInt(1) << i)
            // Values larger than 2^128 are reduced by 2^128 (quick mod)
            if(n > md5Max){
              n -= md5Max
            }
            n
          }).toList
        // Inform server manager that id and table ids are ready
        report(replyTo)
        this
      case SetSuccessor(next, nextId, replyTo) =>
        this.next = next
        this.nextId = nextId
        report(replyTo)
        this
      case SetPrev(prev, prevId, replyTo) =>
        this.prev = prev
        this.prevId = prevId
        report(replyTo)
        this
      case UpdateStats(hops) =>
        this.hops += hops
        this.totalRequests += 1
        this
      case UpdateTable(replyTo) =>
        context.spawnAnonymous(updateTable(context.self, replyTo))
        this
      case UpdatedTable(table, replyTo) =>
        // Update current finger table
        this.table = table
        //context.log.info("Updated table has been received!")
        replyTo ! ServerManager.TableUpdated(context.self)
        this
      case GetId(replyTo) =>
        replyTo ! RespondId(id)
        this
      case FoundSuccessor(successor, id, index, hops) =>
        context.log.info(s"FOUND SUCCESSOR: $successor")
        this
      case Lookup(filename, replyTo) =>
        // Route request to lookup file
        context.spawnAnonymous(lookupFile(context.self, replyTo, filename, 10))
        this
      case GetFile(filename, replyTo, hops) =>
        // Check if the current node has the file requested
        val metadata = data.getOrElse(filename, null)
        if(metadata != null){
          context.log.info(s"FILE FOUND! NAME: $filename, SIZE: ${metadata.getSize}, HOPS: $hops")
          // Respond with the file metadata
          replyTo ! ServerManager.FoundFile(metadata.getFilename, metadata.getSize)
        }
        else{
          context.log.info(s"FILE NOT FOUND! NAME: $filename")
          // Respond file not found
          replyTo ! ServerManager.FileNotFound(filename)
        }
        this
      case Insert(filename, size, replyTo) =>
        val key = new MD5().hash(filename)
        // Cases for inserting at this node
        if((prevId > id && key <= id) || (prevId > id && key > prevId) || (key > prevId && key <= id)){
          // Store metadata in server
          data(filename) = new FileMetadata(filename, size)
          if(replyTo != null ) {replyTo ! ServerManager.FileInserted(filename)}
        }
        else{
          context.spawnAnonymous(insertFile(context.self, replyTo, filename, size))
        }
        this
      case GetData(replyTo) =>
        val avgHops = if (totalRequests == 0) BigInt(0) else hops / totalRequests
        replyTo ! ServerManager.SendData(id, context.self.toString, data.size, totalRequests, avgHops)
        this
    }
  }
}//end class Server

object ServerManager{
  sealed trait Command

  // Command to start the datacenter
  final case class Start(numServers: Int) extends Command
  case object Shutdown extends Command
  final case class TableUpdated(server: ActorRef[Server.Command]) extends Command
  case object TablesUpdated extends Command

  // Commands to set up the datacenter
  case object ServerWarmedUp extends Command
  case object ServersWarmedUp extends Command
  case object ServerReady extends Command
  case object ServersReady extends Command

  final case class FileInserted(filename: String) extends Command
  final case class FoundFile(filename: String, size: Int) extends Command
  final case class FileNotFound(filename: String) extends Command
  // Commands to be used by the web server
  final case class HttpLookUp(movie: String) extends Command
  final case class HttpInsert(movie: String, size: Int) extends Command

  // Commands for snapshot
  final case class SendData(id: BigInt, name: String, numFiles: Int,
                            totalRequests: BigInt, avgHops: BigInt) extends Command
  case object WriteSnapshot extends Command
  case object CancelAllTimers extends Command
}//end object ServerManager

class ServerManager extends Actor with ActorLogging{
  import ServerManager._

  private var ring: List[(BigInt, ActorRef[Server.Command])] = _

  def testInserts(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        val files = (0 to 300).map(x => s"FILE-$x").toList
        val nodes = ring.map(x => x._2)
        context.log.info(s"INSERTING ${files.size} FILE(S)...")
        // Send update table command to all servers
        files.foreach(x =>{
          val node = shuffle(nodes).head
          Thread.sleep(20)
          node ! Server.Insert(x, 200, context.self)
        })
        // Collection for files inserted
        val inserted = mutable.Set[String]()
        // Collection for files found
        val found = mutable.Set[String]()
        // Counter for responses received
        var responses = 0
        Behaviors.receiveMessage{
          case FileInserted(filename) =>
            inserted += filename
            // Update count for responses
            responses += 1
            // Check if all servers have inserted files
            if(responses == files.size){
              // Check that the correct files were reported as successfully inserted
              val correct = files.map(x => if(inserted.contains(x)) 1 else 0).sum
              if(correct == files.size){
                context.log.info(s"ALL ${files.size} FILES HAVE BEEN INSERTED!!")
                Thread.sleep(2000)
                responses = 0
                // Send lookup requests after all files have been inserted
                files.foreach(x => {
                  val node = shuffle(nodes).head
                  Thread.sleep(20)
                  node ! Lookup(x, context.self)
                })
                Behaviors.same
              }
              else{
                context.log.info("ERROR: INCORRECT FILES RETURNED")
                Behaviors.stopped
              }
            }
            else{
              Behaviors.same
            }
          case FoundFile(filename, size) =>
            //found += metadata.getFilename
            found += filename
            responses += 1
            // Check if all files have been found
            if(responses == files.size){
              // Check that the correct files were reported as found
              val correct = files.map(x => if(found.contains(x)) 1 else 0).sum
              if(correct == files.size){
                context.log.info(s"ALL ${files.size} FILES HAVE BEEN FOUND!!")
              }
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
          case FileNotFound(filename) =>
            context.log.info(s"ERROR: FILE NOT FOUND [$filename]")
            Behaviors.stopped
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  def createChordRing(parent: ActorRef[ServerManager.Command],
                      servers: List[ActorRef[Server.Command]]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Create chord ring, sorted by ids
        ring = servers.map(ref => (new MD5().hash(ref.toString), ref)).sortBy(_._1)
        // Sends ids to all servers in the ring
        ring.foreach{ case(id, ref) => ref ! Server.SetId(id, context.self)}
        // Send next and prev ids to servers in the ring
        var prev = ring.last._2
        var prevId = ring.last._1
        // Inform servers about their previous and next nodes in the ring
        ring.foreach{ case(id, ref) =>
          // Set the servers's successor
          prev ! Server.SetSuccessor(ref, id, context.self)
          // Set the servers's predecessor
          ref ! Server.SetPrev(prev, prevId, context.self)
          println(s"REF: $ref, ID: $id")
          // Update previous node in the ring
          prev = ref
          prevId = id
        }
        // Counter for the number of servers that are ready
        var responses = 0
        Behaviors.receiveMessage{
          case ServerWarmedUp =>
            responses += 1
            // Check if all servers have been warmed up
            if(responses == servers.size){
              // Inform server manager that all servers are warmed up
              parent ! ServersWarmedUp
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  def updateTables(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Send update table command to all servers
        ring.foreach{ case(_, ref) => ref ! Server.UpdateTable(context.self) }
        var responses = 0
        Behaviors.receiveMessage{
          case TableUpdated(server) =>
            // Update count for responses
            responses += 1
            // Check if all servers have responded
            if(responses == ring.size){
              // Inform parent that tables have all been created
              parent ! ServersReady
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  def writeSnapshot(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    val serverData = mutable.Map[BigInt, Elem]()
    Behaviors
      .setup[AnyRef]{ context =>
        // Send get data command to all servers
        ring.foreach{ case (_, ref) => ref ! Server.GetData(context.self) }
        var responses = 0
        Behaviors.receiveMessage{
          case SendData(id, name, numFiles, totalRequests, avgHops) =>
            // Add server data to collection
            serverData.put(id, <server><id>{id}</id><name>{name}</name><numFiles>{numFiles}</numFiles><totalRequests>{totalRequests}</totalRequests><avgHops>{avgHops}</avgHops></server>)
            // Update count for responses
            responses += 1
            // Check if all servers have responded
            if (responses == ring.size){
              val servers = new NodeBuffer
              serverData.toSeq.sortBy(_._1).foreach(x => servers += x._2)
              context.log.info("GOT ALL SNAPSHOT RESPONSES. WRITING TO FILE")
              val directory = new File("snapshots")
              if (!directory.exists)
                directory.mkdir
              val pw = new PrintWriter(new File("snapshots/snapshot_" +
                LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")) + ".xml"))
              pw.write(new PrettyPrinter(80, 4).format(<servers>{servers}</servers>))
              pw.close()
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
        }
      }.narrow[NotUsed]
  }

  override def receive: PartialFunction[Any, Unit] ={
    case Start(total) =>
      log.info(s"STARTING $total SERVERS")
      // Create servers for datacenter
      val servers = (1 to total).map(i => context.spawn(Server(), s"server:$i")).toList
      // Create the chord ring
      context.spawnAnonymous(createChordRing(context.self, servers))
    case ServersWarmedUp =>
      log.info("SERVERS WARMED UP")
      // Update Tables
      context.spawnAnonymous(updateTables(context.self))
    case ServersReady =>
      log.info("SERVERS READY")
      Thread.sleep(2000)
      //log.info("TESTING")
      //context.spawnAnonymous(testInserts(context.self))
    case HttpLookUp(movie) =>
      // Select a random node to route request
      val node = shuffle(ring).head._2
      node ! Server.Lookup(movie, sender())
    case HttpInsert(movie, size) =>
      // Select a random node to route request
      val node = shuffle(ring).head._2
      // Insert node in datacenter without response
      node ! Server.Insert(movie, size, null)
    case WriteSnapshot =>
      context.spawnAnonymous(writeSnapshot(context.self))
    case Shutdown =>
      context.stop(self)
  }
}//end class ServerManager

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
            println(input)
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
