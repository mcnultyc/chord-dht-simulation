/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

import akka.actor
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory.load
import org.scalatest.{FlatSpec, Matchers}


// WebServerTests class consisting of unit tests based on the ScalaTest testing framework
class WebServerTests extends FlatSpec with Matchers {
  // TESTS 1-10: read from app configuration
  "Configuration" should "specify akka logger" in {
    load.getStringList("akka.loggers").get(0) should be ("akka.event.slf4j.Slf4jLogger")
  }
  it should "specify akka logging filter" in {
    load.getString("akka.logging-filter") should be ("akka.event.slf4j.Slf4jLoggingFilter")
  }
  it should "specify akka event handlers" in {
    load.getStringList("akka.event-handlers").get(0) should be ("akka.event.slf4j.Slf4jEventHandler")
  }
  it should "specify simulation 1 algorithm" in {
    load.getString("sim1.alg") should be ("simple")
  }
  it should "specify number of servers in simulation 1" in {
    load.getInt("sim1.num-servers") should be (20)
  }
  it should "specify snapshot interval in simulation 1" in {
    load.getInt("sim1.snapshot-interval") should be (15)
  }
  it should "specify simulation 2 algorithm" in {
    load.getString("sim2.alg") should be ("chord")
  }
  it should "specify number of servers in simulation 2" in {
    load.getInt("sim2.num-servers") should be (20)
  }
  it should "specify snapshot interval in simulation 2" in {
    load.getInt("sim2.snapshot-interval") should be (15)
  }

  // TEST 11: actor system
  "ActorSystem" should "be created" in {
    akka.actor.ActorSystem("testing").isInstanceOf[ActorSystem] should be (true)
  }
  // TEST 12-13: server manager
  "ServerManager" should "be created" in {
    akka.actor.ActorSystem("testing").actorOf(Props[ServerManager], "ServerManager").isInstanceOf[actor.ActorRef] should be (true)
  }
  it should "start" in {
    val manager = akka.actor.ActorSystem("testing").actorOf(Props[ServerManager], "ServerManager")
    manager ! ServerManager.Start(20, true)
  }
}//end class WebServerTests
