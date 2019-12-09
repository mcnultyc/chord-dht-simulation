import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.util.Timeout

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.collection.mutable
import java.security.MessageDigest

import Server.{FindSuccessor, Lookup, TestTable, UpdateTable, UpdatedTable}
import akka.NotUsed
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

import scala.util.Random.shuffle

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Await

object MD5{
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
  private var hops: Int = 0
  private var time: Int = 0
  def updateHops(hops: Int): Unit ={
    this.hops= hops
  }
}

// Class to keep metadata for inserts and lookups
class FileMetadata(filename: String, size: Int){
  def getFilename(): String ={
    filename
  }
  def getSize(): Int={
    size
  }
}

object Server{
  sealed trait Command
  // Commands to start and pause the system
  case object Start extends Command
  case object Pause extends Command
  // Commands to update successors and handle routing requests
  final case class SetSuccessor(next: ActorRef[Server.Command], nextId: BigInt,
                                replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class FindSuccessor(replyTo: ActorRef[Server.Command], id: BigInt, index: Int) extends Command
  final case class FoundSuccessor(successor: ActorRef[Command], id: BigInt, index: Int) extends Command

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
  final case class GetFile(filename: String, replyTo: ActorRef[ServerManager.Command]) extends Command

  //final case class FoundFile(data: FileMetadata) extends Command
  final case class FileNotFound(filename: String) extends Command



  case object TestTable extends Command

  // Largest value created by a 128 bit hash such as MD5
  val md5Max = BigInt(1) << 128

  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Server(context))
}

class Server(context: ActorContext[Server.Command])
        extends AbstractBehavior[Server.Command](context) {
  import Server._
  // Actor reference to the next server in the chord ring
  private var next: ActorRef[Server.Command] = null;
  // Cache id of next server in the chord ring
  private var nextId: BigInt = -1
  // Set previous node in chord ring
  private var prev: ActorRef[Server.Command] = null
  // Cache id of the previous server in the chord ring
  private var prevId: BigInt = -1
  // Finger table used for routing messages
  private var table: List[(BigInt, ActorRef[Command])] = null
  // Set id as hash of context username
  private var id: BigInt = -1
  // Store data keys in a set
  private val data = mutable.Map[String, FileMetadata]()
  
  // Ids for finger table entries, n+2^{k-1} for 1 <= k <= 128
  var tableIds: List[BigInt] = null

  /* Find the node in the finger table closest to the ID.
   */
  def closestPrecedingNode(id: BigInt): ActorRef[Command] ={
    // TODO check finger table for closest preceding node
    next // just use next to lookup nodes
  }

  def lookupFile(parent: ActorRef[Command], replyTo: ActorRef[ServerManager.Command], filename: String, size: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        val key = MD5.hash(filename)
        // Find node that should have file
        parent ! FindSuccessor(context.self, key, -1)

        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index) => {
            // Get file from selected node and forward reply
            successor ! GetFile(filename, replyTo)
            Behaviors.stopped
          }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  def insertFile(parent: ActorRef[Command], replyTo: ActorRef[ServerManager.Command], filename: String, size: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        val key = MD5.hash(filename)
        // Find node to insert file in
        parent ! FindSuccessor(context.self, key, -1)

        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index) => {
            // Insert file in located node
            successor ! Insert(filename, size, replyTo)
            Behaviors.stopped
          }
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
        tableIds.foreach( id => {
          index += 1
          next ! FindSuccessor(context.self, id, index)
        })
        val replies = mutable.ListBuffer[(ActorRef[Command], BigInt, Int)]()
        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index) => {
            responses += 1
            replies += ((successor, id, index))
            // Check if all requests have been responded too
            if(responses == tableIds.size){
              // Sort by index
              val table = replies.sortBy(_._3).map{ case(server, id, _) => (id, server)}
              // Send updated table to be processed
              parent ! UpdatedTable(table.toList, replyTo)
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
          }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  /* Behavior for child session to find successor.
   */
  def findSuccessor(parent: ActorRef[Command], replyTo: ActorRef[Command], id: BigInt, index: Int): Behavior[NotUsed] = {
    Behaviors
      .setup[AnyRef] { context =>

        if(prevId > this.id && id <= this.id){ // Case where we insert at first node
          replyTo ! FoundSuccessor(parent, id, index)
          Behaviors.stopped
        }
        else if(prevId > this.id && id > prevId){// Case where insert at first node
          replyTo ! FoundSuccessor(parent, id, index)
          Behaviors.stopped
        }
        else if(id > prevId && id <= this.id){// Case where we insert at this node
          replyTo ! FoundSuccessor(parent, id, index)
          Behaviors.stopped
        }
        else if(id > this.id && id <= nextId){// Case where we insert at next node
          replyTo ! FoundSuccessor(next, id, index)
          Behaviors.stopped
        }
        else if(this.id > nextId && id <= nextId){// Case where we insert at first node
          replyTo ! FoundSuccessor(next, id, index)
          Behaviors.stopped
        }
        else if(this.id > nextId && id > this.id){// Case where we insert at first node
          replyTo ! FoundSuccessor(next, id, index)
          Behaviors.stopped
        }
        else{

          val node = closestPrecedingNode(id)
          // Case where we route request
          node ! FindSuccessor(context.self, id, index)
          Behaviors.receiveMessage{
            case FoundSuccessor(successor, id, index) => {
              // Forward successor to actor that requested successor
              replyTo ! FoundSuccessor(successor,id, index)
              // Stop child session
              Behaviors.stopped
            }
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

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case FindSuccessor(ref, id, index) =>
        // Create child session to handle successor request (concurrent)
        context.spawnAnonymous(findSuccessor(context.self, ref, id, index))
        this
      case SetId(id, replyTo) =>
        this.id = id
        // Create table ids for server's finger table
        tableIds =
          (0 to 127).map(i =>{
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
      case FoundSuccessor(successor,id, index) =>
        context.log.info(s"FOUND SUCCESSOR: $successor")
        this
      case Lookup(filename, replyTo) =>
        // Route request to lookup file
        context.spawnAnonymous(lookupFile(context.self, replyTo, filename, 10))
        this
      case GetFile(filename, replyTo) =>
        // Check if the current node has the file requested
        val metadata = data.getOrElse(filename, null)
        if(metadata != null){
          // Respond with the file metadata
          replyTo ! ServerManager.FoundFile(metadata)
        }
        else{
          // Respond file not found
          replyTo ! ServerManager.FileNotFound(filename)
        }
        this
      case Insert(filename, size, replyTo) =>
        val key = MD5.hash(filename)
        // Cases for inserting at this node
        if(prevId > id && key <= id){
          data(filename) = new FileMetadata(filename, size)
          replyTo ! ServerManager.FileInserted(filename)
        }
        else if(prevId > id && key > prevId){
          data(filename) = new FileMetadata(filename, size)
          replyTo ! ServerManager.FileInserted(filename)
        }
        else if(key > prevId && key <= id){
          data(filename) = new FileMetadata(filename, size)
          replyTo ! ServerManager.FileInserted(filename)
        }
        else{
          context.spawnAnonymous(insertFile(context.self, replyTo, filename, size))
        }
        this
    }
  }

}

object ServerManager{
  sealed trait Command
  // Command to start the datacenter
  final case class Start(numServers: Int) extends Command
  case object Shutdown extends Command
  final case class TableUpdated(server: ActorRef[Server.Command]) extends Command
  case object TablesUpdated extends Command
  case object Test extends Command

  case object ServerWarmedUp   extends Command
  case object ServersWarmedUp extends Command
  case object ServerReady extends Command
  case object ServersReady extends Command

  final case class FileInserted(filename: String) extends Command
  final case class FoundFile(data: FileMetadata) extends Command
  final case class FileNotFound(filename: String) extends Command

  private var ring: List[(BigInt, ActorRef[Server.Command])] = null

  
  def testInserts(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        val files = (0 to 300).map(x => s"FILE#$x").toList
        val nodes = ring.map(x => x._2).toList
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
          case FileInserted(filename) => {
            inserted += filename
            // Update count for responses
            responses += 1
            // Check if all servers have inserted files
            if(responses == files.size){
              // Check that the correct files were reported as successfully inserted
              val correct = files.map(x => if(inserted.contains(x)==true) 1 else 0).sum
              if(correct == files.size){
                context.log.info(s"ALL ${files.size} FILES HAVE BEEN INSERTED!!")
                Thread.sleep(2000)
                responses = 0
                // Send lookup requests after all files have been inserted
                files.foreach(x =>{
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
          }
          case FoundFile(metadata) =>{
            found += metadata.getFilename()
            responses += 1
            // Check if all files have been found
            if(responses == files.size){
              // Check that the correct files were reported as found
              val correct = files.map(x => if(found.contains(x)==true) 1 else 0).sum
              if(correct == files.size){
                context.log.info(s"ALL ${files.size} FILES HAVE BEEN FOUND!!")
              }
              Behaviors.stopped
            }
            else{
              Behaviors.same
            }
          }
          case FileNotFound(filename) => {
            context.log.info(s"ERROR: FILE NOT FOUND [$filename]")
            Behaviors.stopped
          }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

   def createChordRing(parent: ActorRef[ServerManager.Command], servers: List[ActorRef[Server.Command]]): Behavior[NotUsed] = {
     Behaviors
       .setup[AnyRef] { context =>
         // Create chord ring, sorted by ids
         ring = servers.map(ref => (MD5.hash(ref.toString), ref)).sortBy(_._1)
         // Sends ids to all servers in the ring
         ring.foreach{ case(id, ref) => ref ! Server.SetId(id, context.self)}
         // Send next and prev ids to servers in the ring
         var prev = ring.last._2
         var prevId = ring.last._1
         // Inform servers about their previous and next nodes in the ring
         ring.foreach{ case(id, ref) =>{
           // Set the servers's successor
           prev ! Server.SetSuccessor(ref, id, context.self)
           // Set the servers's predecessor
           ref ! Server.SetPrev(prev, prevId, context.self)
           println(s"REF: $ref, ID: $id")
           // Update previous node in the ring
           prev = ref
           prevId = id
         }}
         // Counter for the number of servers that are ready
         var responses = 0
         Behaviors.receiveMessage{
           case ServerWarmedUp =>{
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
           }
           case _ => Behaviors.unhandled
         }
       }.narrow[NotUsed]
   }

  def updateTables(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Send update table command to all servers
        ring.foreach{ case(_, ref) =>ref ! Server.UpdateTable(context.self)}
        var responses = 0
        Behaviors.receiveMessage{
          case TableUpdated(server) => {
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
          }
          case _ => Behaviors.unhandled
        }
      }.narrow[NotUsed]
  }

  def apply(): Behavior[Command] = {

    Behaviors.receive[Command]{
      (context, msg) =>
        msg match {
          case Start(total) =>
            // Create servers for datacenter
            val servers = (1 to total).map(i => context.spawn(Server(), s"server:$i")).toList
            // Create the chord ring
            context.spawnAnonymous(createChordRing(context.self, servers))
            Behaviors.same
          case ServersReady =>
            context.log.info("SERVERS READY")
            Thread.sleep(2000)
            context.spawnAnonymous(testInserts(context.self))
            Behaviors.same
          case FileInserted(filename) =>
            val last = ring.last._2
            last ! Lookup("nailingpailin", context.self)
            Behaviors.same
          case ServersWarmedUp =>
            context.log.info("SERVERS WARMED UP")
            // Update Tables
            context.spawnAnonymous(updateTables(context.self))
            Behaviors.same
          case FoundFile(metadata) =>
            context.log.info(s"FOUND FILE!!, FILENAME: ${metadata.getFilename()}, SIZE: ${metadata.getSize()}")
            Behaviors.same
          case Test =>
            val last = ring.last._2
            val first = ring.head._2
            context.log.info(s"LAST: $last")
            //last ! Server.FindSuccessor(first, MD5.hash("nailingpailin"))
            Behaviors.same
          case Shutdown =>
            Behaviors.stopped
        }
    }
  }
}

/*
object Driver{

  def main(args: Array[String]): Unit = {
    val tests = new TestChord()
    tests.runTest()
  }
}
*/


object Driver extends App {
  val system = ActorSystem(ServerManager(), "chord")
  // Add 5 servers to the system
  system ! ServerManager.Start(20)
  // Sleep for 7 seconds and then send shutdown signal
  Thread.sleep(30000)
  system ! ServerManager.Shutdown
}
