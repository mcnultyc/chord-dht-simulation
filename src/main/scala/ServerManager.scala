/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import Server.Lookup
import akka.NotUsed
import akka.actor.{Actor, ActorLogging}
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import scala.collection.mutable
import scala.util.Random.shuffle
import scala.xml.{Elem, NodeBuffer, PrettyPrinter}


object ServerManager{
  sealed trait Command

  // Command to start the datacenter
  final case class Start(numServers: Int, enableTable: Boolean) extends Command
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
                            totalRequests: BigInt, avgHops: Float) extends Command
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
                      servers: List[ActorRef[Server.Command]],
                      enableTable: Boolean): Behavior[NotUsed] ={
    Behaviors
      .setup[AnyRef]{ context =>
        // Create chord ring, sorted by ids
        ring = servers.map(ref => (new MD5().hash(ref.toString), ref)).sortBy(_._1)
        // Sends ids to all servers in the ring
        ring.foreach{ case(id, ref) => ref ! Server.SetId(id, context.self, enableTable)}
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
    val serverData = mutable.Map[BigInt, (Elem, Int, Float)]()
    Behaviors
      .setup[AnyRef]{ context =>
        // Send get data command to all servers
        ring.foreach{ case (_, ref) => ref ! Server.GetData(context.self) }
        var responses = 0
        Behaviors.receiveMessage{
          case SendData(id, name, numFiles, totalRequests, avgHops) =>
            // Add server data to collection
            serverData.put(id, (<server><id>{id}</id><name>{name}</name><numFiles>{numFiles}</numFiles><totalRequests>{totalRequests}</totalRequests><avgHops>{avgHops}</avgHops></server>, numFiles, avgHops))
            // Update count for responses
            responses += 1
            // Check if all servers have responded
            if (responses == ring.size){
              val servers = new NodeBuffer
              servers += <numFiles>{serverData.map(_._2._2).sum}</numFiles>
              servers += <avgHops>{serverData.map(_._2._3).sum / serverData.size}</avgHops>
              serverData.toSeq.sortBy(_._1).foreach(x => servers += x._2._1)
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
    case Start(total, enableTable) =>
      log.info(s"STARTING $total SERVERS, TABLES ENABLED: $enableTable")
      // Create servers for datacenter
      val servers = (1 to total).map(i => context.spawn(Server(), s"server:$i")).toList
      // Create the chord ring
      context.spawnAnonymous(createChordRing(context.self, servers, enableTable))
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