import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.util.Timeout

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.collection.mutable
import java.security.MessageDigest

import Server.{FindSuccessor, TestTable, UpdateTable, UpdatedTable}
import akka.NotUsed
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

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
  final case class GetSuccessor(id: BigInt) extends Command
  final case class SetSuccessor(next: ActorRef[Server.Command], nextId: BigInt) extends Command
  final case class FindSuccessor(replyTo: ActorRef[Server.Command], id: BigInt) extends Command
  final case class FoundSuccessor(successor: ActorRef[Command], id: BigInt) extends Command

  final case class SetPrev(prev: ActorRef[Server.Command], prevId: BigInt) extends Command

  // Commands to update server ids
  final case class SetId(id: BigInt) extends Command
  final case class GetId(replyTo: ActorRef[Command]) extends Command
  final case class RespondId(id: BigInt) extends Command
  // Commands to update table and respond updated table
  final case class UpdateTable(replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class UpdatedTable(table: List[(BigInt, ActorRef[Command])],
                                replyTo: ActorRef[ServerManager.Command]) extends Command
  // Commands to insert and lookup data
  final case class Insert(filename: String, size: Int) extends Command
  final case class Lookup(key: BigInt) extends Command
  final case class FoundData(data: String) extends Command

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
  private var nextId: BigInt = 0
  // Set previous node in chord ring
  private var prev: ActorRef[Server.Command] = null
  // Cache id of the previous server in the chord ring
  private var prevId: BigInt = 0
  // Finger table used for routing messages
  private var table: List[(BigInt, ActorRef[Command])] = null
  // Set id as hash of context username
  private var id: BigInt = MD5.hash(context.self.toString)
  // Store data keys in a set
  private val data = mutable.Set[FileMetadata]()
  private var printLog = false
  // Ids for finger table entries, n+2^{k-1} for 1 <= k <= 128
  private val tableIds =
    (0 to 127).map(i =>{
      var n = this.id + (BigInt(1) << i)
      // Values larger than 2^128 are reduced by 2^128 (quick mod)
      if(n > md5Max){
        n -= md5Max
      }
      n
  })

  /* Find the node in the finger table closest to the ID.
   */
  def closestPrecedingNode(id: BigInt): ActorRef[Command] ={
    // TODO check finger table for closest preceding node
    if(table != null){
      // Iterate through finger from largest to smallest key
      table.reverse.foreach{case (fingerId, ref) =>{
        // Select node with highest key that can fit the id given
        if(fingerId >= this.id && fingerId <= id){
          //context.log.info(s"Closest preceding node: $ref")
          return ref
        }
      }}
    }
    next // just use next to lookup nodes
  }


  def insertFile(parent: ActorRef[Command], filename: String, size: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        context.log.info("In insert file function")
        val key = MD5.hash(filename)
        // Find node to insert file in
        parent ! FindSuccessor(context.self, key)

        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id) => {
            context.log.info(s"FOUND LOCATION FOR FILE: $successor")
            // Insert file in located node
            successor ! Insert(filename, size)
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
        tableIds.foreach( id => next ! FindSuccessor(context.self, id))
        val table = mutable.ListBuffer[(BigInt, ActorRef[Command])]()
        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id) => {
            responses += 1
            table += ((id, successor))
            // Check if all requests have been responded too
            if(responses == tableIds.size){
              // Send updated table to be processed
              parent ! UpdatedTable(table.sortBy(_._1).toList, replyTo)
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
  def findSuccessor(parent: ActorRef[Command], replyTo: ActorRef[Command], id: BigInt): Behavior[NotUsed] = {
    Behaviors
      .setup[AnyRef] { context =>
        if(printLog)context.log.info(s"In find successor function, REF: $parent")
        // Check if in node with largest id in chord ring
        if(this.id > nextId){
          // Check if id greater than largest chord ring or
          // less than the smallest id
          if(id > this.id || id <= nextId){
            //context.log.info(s"Successor found at: $parent")
            if(printLog){
              context.log.info(s"LOCATION FOR FILE: $parent")
            }
            replyTo ! FoundSuccessor(next, id)
            Behaviors.stopped
          }
          else{
            // Find the closest preceding node
            val node = closestPrecedingNode(id)
            if(printLog){
              context.log.info(s"ClOSEST PRECEDING NODE(1): $node")
            }
            // Request successor from closest preceding node
            node ! FindSuccessor(context.self, id)
            Behaviors.receiveMessage{
              case FoundSuccessor(successor, id) => {
                // Forward successor to actor that requested successor
                replyTo ! FoundSuccessor(successor,id)
                // Stop child session
                Behaviors.stopped
              }
              case _ => Behaviors.unhandled
            }
          }
        }
        else{
          // Check if id falls in range (this.id, id]
          if(id > this.id && id <= nextId){
            //context.log.info(s"Successor found at: $parent")
            // Forward our next to actor that requested successor
            if(printLog){
              context.log.info(s"FILE LOCATION: $parent")
            }
            replyTo ! FoundSuccessor(next, id)
            // Stop child session
            Behaviors.stopped
          }
          else{
            // Find the closest preceding node
            val node = closestPrecedingNode(id)
            if(printLog){
              context.log.info(s"CLOSEST PRECEDING NODE(2): $node")
            }
            // Request successor from closest preceding node
            node ! FindSuccessor(context.self, id)
            Behaviors.receiveMessage{
              case FoundSuccessor(successor, id) => {
                // Forward successor to actor that requested successor
                replyTo ! FoundSuccessor(successor, id)
                // Stop child session
                Behaviors.stopped
              }
              case _ => Behaviors.unhandled
            }
          }
        }
      }.narrow[NotUsed]
  }

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case FindSuccessor(ref,id) =>
        if(printLog) {
          context.log.info(s"FIND SUCCESSOR - PREV $ref, THIS: ${context.self}")
        }
        //context.log.info(s"Find successor at ${context.self}")
        // Create child session to handle successor request (concurrent)
        context.spawnAnonymous(findSuccessor(context.self, ref, id))
        this
      case SetId(id) =>
        this.id = id
        this
      case SetSuccessor(next, nextId) =>
        this.next = next
        this.nextId = nextId
        this
      case UpdateTable(replyTo) =>
        // TODO verify that finger table entries are correct
        context.spawnAnonymous(updateTable(context.self, replyTo))
        this
      case UpdatedTable(table, replyTo) =>
        println(s"ID: $id, NEXT: $nextId, PREV: $prevId")
        // Update current finger table
        this.table = table
        //println(s"*******************************ref: ${context.self}*******************************")
        this.table.foreach{ case(id, ref) =>{
          //println(s"id: $id, ref: $ref")
        }}
        //context.log.info("Updated table has been received!")
        replyTo ! ServerManager.TableUpdated(context.self)
        this
      case TestTable =>
        val testId = BigInt("333658623028670999750805885164791678408")
        context.spawnAnonymous(findSuccessor(context.self, context.self, testId))
        this
      case GetId(replyTo) =>
        replyTo ! RespondId(id)
        this
      case SetPrev(prev, prevId) =>
        this.prev = prev
        this.prevId = prevId
        this
      case FoundSuccessor(successor,id) =>
        context.log.info(s"FOUND SUCCESSOR: $successor")
        this
      case Insert(filename, size) =>
        printLog = true
        context.log.info(s"STARTING AT: ${context.self}")
        val key = MD5.hash(filename)
        context.log.info(s"FILE KEY: $key")
        // TODO bug for inserting at head
        if(prevId > id && key <= id){
          data += new FileMetadata(filename, size)
          context.log.info(s"INSERT FILE AT: ${context.self}")
        }
        else if(key > prevId && key <= id){
          data += new FileMetadata(filename, size)
          context.log.info(s"INSERT FILE AT: ${context.self}")
        }
        else{
          context.spawnAnonymous(insertFile(context.self, filename, size))
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

  private var chordRing: List[(BigInt, ActorRef[Server.Command])] = null

  def createChordRing(chordRing: List[(BigInt, ActorRef[Server.Command])]): Unit ={
    // Set prev of first node to be the last node (circular list)
    var prev = chordRing.last._2
    // Set prev id to be id of last node in the ring
    var prevId = chordRing.last._1
    // Set successors for nodes in chord ring
    //println(s"id: ${chordRing.last._1}")
    chordRing.foreach {
      case (id, ref) => {
        prev ! Server.SetSuccessor(ref, id)
        ref ! Server.SetPrev(prev, prevId)
        println(s"REF: $ref, ID: $id")
        prev = ref
        prevId = id
      }
    }
  }

  def updateTables(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Send update table command to all servers
        chordRing.foreach{ case(_, ref) =>ref ! Server.UpdateTable(context.self)}
        var responses = 0
        Behaviors.receiveMessage{
          case TableUpdated(server) => {
            // Update count for responses
            responses += 1
            // Check if all servers have responded
            if(responses == chordRing.size){
              context.log.info(s"Servers have updated tables!")
              // Inform parent that tables have all been created
              parent ! TablesUpdated
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
            val servers =
              (1 to total).map(i => {
                // Hash server name to create id
                val ref = context.spawn(Server(), s"server:$i")
                val id = MD5.hash(ref.toString)
                ref ! Server.SetId(id)
                (id, ref)
              })
            // Sort chord ring by server ids
            chordRing = servers.sortBy(_._1).toList
            // Create chord ring from server hashes
            createChordRing(chordRing)
            // Update Tables
            context.spawnAnonymous(updateTables(context.self))
            Behaviors.same
          case TablesUpdated =>
            val ref = chordRing.head._2
            context.log.info("************In Tables updated handler***************")
            context.log.info(s"FIRST: $ref")
            println(s"FILE KEY: ${MD5.hash("nailingpailin")}")
            ref ! Server.Insert("nailingpailin", 5000)
            //context.self ! Test
            Behaviors.same
          case Test =>
            val last = chordRing.last._2
            val first = chordRing.head._2
            context.log.info(s"LAST: $last")
            last ! Server.FindSuccessor(first, MD5.hash("nailingpailin"))
            Behaviors.same
          case Shutdown =>
            Behaviors.stopped
        }
    }
  }
}

object Driver extends App {
  val system = ActorSystem(ServerManager(), "chord")
  // Add 5 servers to the system
  system ! ServerManager.Start(5)
  // Sleep for 7 seconds and then send shutdown signal

}