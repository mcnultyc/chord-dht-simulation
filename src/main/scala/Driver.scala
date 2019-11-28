import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import java.security.MessageDigest

import javax.xml.bind.annotation.adapters.HexBinaryAdapter

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
  final case class SetSuccessor(next: ActorRef[Server.Command]) extends Command
  final case class SetId(id: BigInt) extends Command
  case object  GetId extends Command
  case object UpdateTable extends Command
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Server(context))
}

class Server(context: ActorContext[Server.Command])
        extends AbstractBehavior[Server.Command](context){
  import Server._
  private var next: ActorRef[Server.Command] = null;
  private var id: BigInt = 0

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case SetId(id) =>
        this.id = id
        this
      case SetSuccessor(next) =>
        this.next = next
        this
    }
  }
}

object ServerManager{
  final case class Add(total: Int)
  private var numServers = 1

  def createChordRing(servers: List[(BigInt, ActorRef[Server.Command])]): Unit ={
    val flatChordRing = servers.sortBy(_._1)
    flatChordRing.foreach{
      case (id, ref) => {
        println(s"id: $id, ref: $ref")
      }
    }

    var prev = flatChordRing.last._2
    var prevId = flatChordRing.last._1
    flatChordRing.foreach{
      case (id, ref) =>{
        println(s"$prevId, next: $id")
        prevId = id
        prev ! Server.SetSuccessor(ref)
        prev = ref
      }
    }
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

class ServerManager(context: ActorContext[String]) extends AbstractBehavior[String](context){
  override def onMessage(msg: String): Behavior[String] = Behaviors.unhandled
}

object Driver extends App {
  val system = ActorSystem(ServerManager(), "chord")
  system ! ServerManager.Add(5)
}