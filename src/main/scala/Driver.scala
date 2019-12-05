import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.util.Timeout

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.collection.mutable
import java.security.MessageDigest

import Server.{FindSuccessor, UpdateTable}
import akka.NotUsed
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

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

object Server{
  sealed trait Command

  case object Start extends Command
  case object Pause extends Command

  final case class GetSuccessor(id: BigInt) extends Command
  final case class SetSuccessor(next: ActorRef[Server.Command], nextId: BigInt) extends Command
  final case class FindSuccessor(replyTo: ActorRef[Server.Command], id: BigInt) extends Command
  final case class FoundSuccessor(successor: ActorRef[Command]) extends Command

  final case class SetId(id: BigInt) extends Command
  final case class GetId(replyTo: ActorRef[Command]) extends Command
  final case class RespondId(id: BigInt) extends Command

  case object UpdateTable extends Command


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
  // Ids for finger table entries, n+2^{k-1} for 1 <= k <= 128
  private var tableIds =
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
    next // just use next to lookup nodes for now (simple ring)
  }

  def findSuccessor(parent: ActorRef[Command], replyTo: ActorRef[Command], id: BigInt): Behavior[NotUsed] = {
    Behaviors
      .setup[AnyRef] { context =>
        // Check if in node with largest id in chord ring
        if(this.id > nextId){
          // Check if id greater than largest chord ring or
          // less than the smallest id
          if(id > this.id || id <= nextId){
            replyTo ! FoundSuccessor(next)
            Behaviors.stopped
          }
          else{
            // Find the closest preceding node
            val node = closestPrecedingNode(id)
            // Request successor from closest preceding node
            node ! FindSuccessor(context.self, id)
            Behaviors.receiveMessage{
              case FoundSuccessor(successor) => {
                // Forward successor to actor that requested successor
                replyTo ! FoundSuccessor(successor)
                // Stop child session
                Behaviors.stopped
              }
              case _ => {Behaviors.unhandled}
            }
          }
        }
        else{
          // Check if id falls in range (this.id, id]
          if(id > this.id && id <= nextId){
            // Forward our next to actor that requested successor
            replyTo ! FoundSuccessor(next)
            // Stop child session
            Behaviors.stopped
          }
          else{
            // Find the closest preceding node
            val node = closestPrecedingNode(id)
            // Request successor from closest preceding node
            node ! FindSuccessor(context.self, id)
            Behaviors.receiveMessage{
              case FoundSuccessor(successor) => {
                // Forward successor to actor that requested successor
                replyTo ! FoundSuccessor(successor)
                // Stop child session
                Behaviors.stopped
              }
              case _ => {Behaviors.unhandled}
            }
          }
        }
      }.narrow[NotUsed]
  }

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case FindSuccessor(ref,id) =>
        // Create child session to handle successor request (concurrent)
        context.spawn(findSuccessor(context.self, ref, id), s"finding-successor-$id")
        this
      case SetId(id) =>
        this.id = id
        this
      case SetSuccessor(next, nextId) =>
        this.next = next
        this.nextId = nextId
        this
      case UpdateTable =>
        // TODO set finger table nodes
        // Create child session to handle successor request (concurrent)
        val testID = BigInt("75669289783886579685404451884628016793")
        context.log.info(s"Finding successor to $testID...")
        context.spawn(findSuccessor(context.self, context.self, testID),
          "finding-successor")
        this
      case GetId(replyTo) =>
        replyTo ! RespondId(id)
        this
      case FoundSuccessor(successor) =>
        context.log.info(s"Found successor: $successor")
        this
    }
  }

}

object ServerManager{
  sealed trait Command
  final case class Add(total: Int) extends Command
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
    val last = flatChordRing.last._2
    last ! UpdateTable
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