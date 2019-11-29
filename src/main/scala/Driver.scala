import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import java.security.MessageDigest

import javax.xml.bind.annotation.adapters.HexBinaryAdapter

import scala.collection.mutable

object MD5{
  private val hashAlgorithm = MessageDigest.getInstance("MD5")
  private val hexAdapter = new HexBinaryAdapter()
  def hash(value: String): BigInt ={
    val bytes = hashAlgorithm.digest(value.getBytes())
    val hex = hexAdapter.marshal(bytes)
    BigInt(hex, 16)
  }
}

object Server{
  sealed trait Command
  case object Start extends Command
  case object Pause extends Command

  final case class GetSuccessor(id: BigInt) extends Command
  final case class SetSuccessor(next: ActorRef[Server.Command], nextId: BigInt) extends Command
  final case class SetTableSuccessor(node: ActorRef[Server.Command], close: BigInt, id: BigInt) extends Command
  final case class FindSuccessor(id: BigInt) extends Command

  final case class SetId(id: BigInt) extends Command
  final case class GetId(replyTo: ActorRef[Command]) extends Command
  final case class RespondId(id: BigInt) extends Command

  case object GetNextId extends Command
  case object UpdateTable extends Command

  val mod = BigInt(1) << 128

  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Server(context))
}

class Server(context: ActorContext[Server.Command])
        extends AbstractBehavior[Server.Command](context) {
  import Server._
  private var next: ActorRef[Server.Command] = null;
  private var id: BigInt = MD5.hash(context.self.toString)
  private var nextId: BigInt = 0
  private var table = mutable.ListBuffer[(BigInt, ActorRef[Command])]()

  (0 to 127).foreach(i =>{
    var n = this.id + (BigInt(1) << i)
    if(n > mod){
      n -= mod
    }
    table += ((n, null))
  })

  def findSuccessor(id: BigInt){
    if(id > this.id && id <= nextId){
      //return next
    }
    else{
      //val node = closestPrecedingNode(id)
      // TODO message node to find successor
      //node ! FindSuccessor(id)
    }
  }
  /*
  def closestPrecedingNode(id: BigInt): ActorRef[Command] ={

  }

   */

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case SetId(id) =>
        this.id = id
        this
      case SetSuccessor(next, nextId) =>
        this.next = next
        this.nextId = nextId
        this
      case UpdateTable =>

        this
      case GetId(replyTo) =>
        replyTo ! RespondId(id)
        this
    }
  }
}

object ServerManager{
  final case class Add(total: Int)
  final case class RespondId(id: BigInt)
  private var numServers = 1

  def createChordRing(servers: List[(BigInt, ActorRef[Server.Command])]): Unit ={
    val flatChordRing = servers.sortBy(_._1)
    var prev = flatChordRing.last._2
    flatChordRing.foreach{
      case (id, ref) =>{
        prev ! Server.SetSuccessor(ref, id)
        prev = ref
      }
    }
    val last = flatChordRing.last._2
    last ! Server.UpdateTable
  }

  def apply(): Behavior[Add] = Behaviors.receive{ (context, msg) =>
    val servers =
      (numServers to msg.total).map(i => {
        val ref = context.spawn(Server(), s"server-$i")
        val id = MD5.hash(ref.toString)
        ref ! Server.SetId(id)
        (id, ref)
      })
    createChordRing(servers.toList)
    numServers += msg.total
    Behaviors.same
  }
}

object Driver extends App {
  val system = ActorSystem(ServerManager(), "chord")
  system ! ServerManager.Add(5)
}