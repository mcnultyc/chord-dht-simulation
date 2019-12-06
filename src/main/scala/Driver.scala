import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.util.Timeout

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.collection.mutable
import java.security.MessageDigest

import Server.{FindSuccessor, TestTable, UpdateTable}
import akka.NotUsed
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

import scala.compat.java8.FutureConverters.CompletionStageOps

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
  // Commands to update server ids
  final case class SetId(id: BigInt) extends Command
  final case class GetId(replyTo: ActorRef[Command]) extends Command
  final case class RespondId(id: BigInt) extends Command
  // Commands to update table and respond updated table
  case object UpdateTable extends Command
  final case class UpdatedTable(table: List[(BigInt, ActorRef[Command])]) extends Command
  // Commands to insert and lookup data
  final case class Insert(data: String) extends Command
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
  // Finger table used for routing messages
  private var table: List[(BigInt, ActorRef[Command])] = null
  // Set id as hash of context username
  private var id: BigInt = MD5.hash(context.self.toString)
  // Store data keys in a set
  private val data = mutable.Set[String]()
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
    /*
    // TODO check finger table for closest preceding node
    if(table != null){
      //context.log.info("TABLE IS NON-NULL")
      // Iterate through finger from largest to smallest key
      table.reverse.foreach{case (fingerId, ref) =>{
        // Select node with highest key that can fit the id given
        if(fingerId >= this.id && fingerId <= id){
          //context.log.info(s"Closest preceding node: $ref")
          return ref
        }
      }}
    }
    else{
      //context.log.info(s"TABLE IS NULL")
    }
    */
    next // just use next to lookup nodes
  }

  def updateTable(parent: ActorRef[Command]): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef] { context =>
        // Counter for the number of responses
        var responses = 0
        // Request successor for each entry in finger table through immediate next
        tableIds.foreach( id => next ! FindSuccessor(context.self, id))
        val table = mutable.ListBuffer[(BigInt, ActorRef[Command])]()
        Behaviors.receiveMessage{
          case FoundSuccessor(successor, id) => {
            //context.log.info(s"$id: $successor, ${responses+1}")
            responses += 1
            table += ((id, successor))
            // Check if all requests have been responded too
            if(responses == tableIds.size){
              // Send updated table to be processed
              parent ! UpdatedTable(table.sortBy(_._1).toList)
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
        // Check if in node with largest id in chord ring
        if(this.id > nextId){
          // Check if id greater than largest chord ring or
          // less than the smallest id
          if(id > this.id || id <= nextId){
            context.log.info(s"Successor found at: $parent")
            replyTo ! FoundSuccessor(next, id)
            Behaviors.stopped
          }
          else{
            //context.log.info("Calling preceding node")
            // Find the closest preceding node
            val node = closestPrecedingNode(id)
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
            context.log.info(s"Successor found at: $parent")
            // Forward our next to actor that requested successor
            replyTo ! FoundSuccessor(next, id)
            // Stop child session
            Behaviors.stopped
          }
          else{
            //context.log.info("Calling preceding node")
            // Find the closest preceding node
            val node = closestPrecedingNode(id)
            //context.log.info(s"Forwarding request to: $node")
            // Request successor from closest preceding node
            node ! FindSuccessor(context.self, id)
            Behaviors.receiveMessage{
              case FoundSuccessor(successor, id) => {
                //context.log.info(s"Forwarding back to: $replyTo")
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
      case UpdateTable =>
        // TODO verify that finger table entries are correct
        context.spawnAnonymous(updateTable(context.self))
        this
      case UpdatedTable(table) =>
        // Update current finger table
        this.table = table
        context.log.info("Updated table has been received!")
        this
      case TestTable =>
        val testId = BigInt("333658623028670999750805885164791678408")
        context.spawnAnonymous(findSuccessor(context.self, context.self, testId))
        this
      case GetId(replyTo) =>
        replyTo ! RespondId(id)
        this
      case FoundSuccessor(successor,id) =>
        context.log.info(s"Found successor: $successor")
        this
    }
  }

}

object ServerManager{
  sealed trait Command
  final case class Add(total: Int) extends Command
  case object CreateTables extends Command
  case object Shutdown extends Command

  def createChordRing(servers: List[(BigInt, ActorRef[Server.Command])]): Unit ={
    // Sort chord ring by server ids
    val flatChordRing = servers.sortBy(_._1)
    // Set prev of first node to be the last node (circular list)
    var prev = flatChordRing.last._2
    // Set successors for nodes in chord ring
    println(s"id: ${flatChordRing.last._1}")
    flatChordRing.foreach{
      case (id, ref) =>{
        prev ! Server.SetSuccessor(ref, id)
        println(s"prev: $prev, next: $ref, id: $id")
        prev = ref
      }
    }
    Thread.sleep(2000)
    flatChordRing.foreach{
      case(_, ref) =>{
      //  ref ! UpdateTable
      }
    }
    flatChordRing.last._2 ! UpdateTable
    Thread.sleep(500)
    flatChordRing.last._2 ! TestTable
  }

  def apply(): Behavior[Command] = {

    Behaviors.receive[Command]{
      (context, msg) =>
        msg match {
          case Add(total) =>
            val servers =
              (1 to total).map(i => {
                // Hash server name to create id
                val ref = context.spawn(Server(), s"server:$i")
                val id = MD5.hash(ref.toString)
                ref ! Server.SetId(id)
                (id, ref)
              })
            // Create chord ring from server hashes
            createChordRing(servers.toList)
            Behaviors.same
          case CreateTables =>
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
  system ! ServerManager.Add(5)
  // Sleep for 7 seconds and then send shutdown signal
  Thread.sleep(7000)
  system ! ServerManager.Shutdown
}