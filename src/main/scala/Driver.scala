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

class TestChord{

  def select(id: Int, node: Int, prev: Int, next: Int): Int ={
    if(prev > node && id <= node){ // Case where we insert at first node
      node
    }
    else if(prev > node && id > prev){// Case where insert at first node
      node
    }
    else if(id > prev && id <= node){// Case where we insert at this node
      node
    }
    else if(id > node && id <= next){// Case where we insert at next node
      next
    }
    else if(node > next && id <= next){// Case where we insert at first node
      next
    }
    else if(node > next && id > node){// Case where we insert at first node
      next
    }
    else{
      -1
    }
  }

  def runTest(): Unit ={
    val ring = List((0, 100, 10))++(10 to 90 by 10).map(x=>(x, x-10,x+10))++List((100, 90, 0))
    val nexts = mutable.Map[Int, Int]()
    (0 to 90 by 10).foreach(x => {
      (x+1 to x+10).foreach(y =>{
        nexts(y) = x+10
      })
    })
    (101 to 200).foreach(x => nexts(x) = 0)
    nexts(0) = 0
    var id = ring.head._1
    val passed =
    ring.tail.map{case(node,prev, next) =>{
      id += 1
      val res = select(id, node, prev, next)
      val temp = id
      id = node
      if(nexts(temp) != res) 0 else 1
    }}.sum

    val edgeCases = if(select(101,0,100, 10) == 0) 1 else 0

    println(passed+edgeCases)
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
  final case class FindSuccessor(replyTo: ActorRef[Server.Command], id: BigInt, index: Int) extends Command
  final case class FoundSuccessor(successor: ActorRef[Command], id: BigInt, index: Int) extends Command

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
  final case class Insert(filename: String, size: Int, replyTo: ActorRef[ServerManager.Command]) extends Command

  final case class Lookup(filename: String, replyTo: ActorRef[ServerManager.Command]) extends Command
  final case class GetFile(filename: String, replyTo: ActorRef[ServerManager.Command]) extends Command

  //final case class FoundFile(data: FileMetadata) extends Command
  final case class FileNotFound(filename: String) extends Command

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
  private val data = mutable.Map[String, FileMetadata]()
  
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
    next // just use next to lookup nodes
  }

  def lookupFile(parent: ActorRef[Command], replyTo: ActorRef[ServerManager.Command], filename: String, size: Int): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        //context.log.info(s"IN LOOKUP FILE FUNCTION: ${context.self}")
        val key = MD5.hash(filename)
        // Find node that should have file
        parent ! FindSuccessor(context.self, key, -1)

        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index) => {
            //context.log.info(s"FOUND LOCATION FOR FILE: $successor")
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
        //context.log.info(s"IN INSERT FILE FUNCTION: ${context.self}")
        val key = MD5.hash(filename)
        // Find node to insert file in
        parent ! FindSuccessor(context.self, key, -1)

        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id, index) => {
            //context.log.info(s"FOUND LOCATION FOR FILE: $successor")
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

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case FindSuccessor(ref, id, index) =>
        //context.log.info(s"FIND SUCCESSOR - PREV $ref, THIS: ${context.self}")
        //context.log.info(s"Find successor at ${context.self}")
        // Create child session to handle successor request (concurrent)
        context.spawnAnonymous(findSuccessor(context.self, ref, id, index))
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
        // Update current finger table
        this.table = table
        //context.log.info("Updated table has been received!")
        replyTo ! ServerManager.TableUpdated(context.self)
        this
      case GetId(replyTo) =>
        replyTo ! RespondId(id)
        this
      case SetPrev(prev, prevId) =>
        this.prev = prev
        this.prevId = prevId
        this
      case FoundSuccessor(successor,id, index) =>
        context.log.info(s"FOUND SUCCESSOR: $successor")
        this
      case Lookup(filename, replyTo) =>
        //context.log.info(s"LOOKING UP FILE: $filename")
        // Route request to lookup file
        context.spawnAnonymous(lookupFile(context.self, replyTo, filename, 10))
        this
      case GetFile(filename, replyTo) =>
        // Check if the current node has the file requested
        val metadata = data.getOrElse(filename, null)
        if(metadata != null){
          //context.log.info(s"FOUND FILE AT: ${context.self}")
          // Respond with the file metadata
          replyTo ! ServerManager.FoundFile(metadata)
        }
        else{
          //context.log.info(s"FILE NOT FOUND AT: ${context.self}")
          // Respond file not found
          replyTo ! ServerManager.FileNotFound(filename)
        }
        this
      case Insert(filename, size, replyTo) =>
        val key = MD5.hash(filename)
        //context.log.info(s"FILE KEY: $key")
        if(prevId > id && key <= id){
          data(filename) = new FileMetadata(filename, size)
          //context.log.info(s"INSERT FILE AT: ${context.self}, ID: $id")
          replyTo ! ServerManager.FileInserted(filename)
        }
        else if(prevId > id && key > prevId){
          data(filename) = new FileMetadata(filename, size)
          //context.log.info(s"INSERT FILE AT: ${context.self}, ID: $id")
          replyTo ! ServerManager.FileInserted(filename)
        }
        else if(key > prevId && key <= id){
          data(filename) = new FileMetadata(filename, size)
          //context.log.info(s"INSERT FILE AT: ${context.self}")
          replyTo ! ServerManager.FileInserted(filename)
        }
        else{
          //context.log.info(s"ROUTING REQUEST THROUGH: ${context.self}, ID: $id")
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

  final case class FileInserted(filename: String) extends Command
  final case class FoundFile(data: FileMetadata) extends Command
   final case class FileNotFound(filename: String) extends Command 

  private var chordRing: List[(BigInt, ActorRef[Server.Command])] = null

  def testInserts(parent: ActorRef[ServerManager.Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Send update table command to all servers
        val files = (0 to 300).map(x => s"FILE#$x").toList
        val nodes = chordRing.map(x => x._2).toList
        context.log.info(s"INSERTING ${files.size} FILE(S)...")
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
            Thread.sleep(10000)
            context.spawnAnonymous(testInserts(context.self))
            //context.log.info(s"FIRST: $ref")
            //println(s"FILE KEY: ${MD5.hash("nailingpailin")}")
            //ref ! Server.Insert("nailingpailin", 5000, context.self)
            //context.self ! Test
            Behaviors.same
          case FileInserted(filename) =>
            val last = chordRing.last._2
            last ! Lookup("nailingpailin", context.self)
            Behaviors.same
          case FoundFile(metadata) =>
            context.log.info(s"FOUND FILE!!, FILENAME: ${metadata.getFilename()}, SIZE: ${metadata.getSize()}")
            Behaviors.same
          case Test =>
            val last = chordRing.last._2
            val first = chordRing.head._2
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
