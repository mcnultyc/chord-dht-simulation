/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import scala.collection.mutable


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