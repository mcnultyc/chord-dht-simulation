# CS441 Course Project - Chord Algorithm Akka/HTTP-based Simulator
#### Description: Create a Chord cloud overlay network algorithm with convergent hashing using Akka/HTTP-based simulator.

#### Team: Carlos Antonio McNulty (cmcnul3), Abram Gorgis (agorgi2), Priyan Sureshkumar (psures5), Shyam Patel (spate54)

#
##### Driver Language:          Scala 2.13.1
##### Testing Framework:        ScalaTest
##### Building Framework:       SBT
##### Configuration Library:    Typesafe
##### Other:       
                            1 simulation .conf files with name(s) XXXXXXXXXXXX.conf
#
                            
##### Main Driver Class:        Driver in Driver.scala

##### Required Library Dependencies:
```
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % "10.1.11",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.11",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
```
                            
#### To Compile: 

##### To Run Simulation(s):
                         
##### To Run Tests(in test directory): sbt test