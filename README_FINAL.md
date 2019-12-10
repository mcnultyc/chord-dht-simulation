# CS 441 Course Project - Chord/Akka Model
#### Description: Create a Chord cloud overlay network algorithm with convergent hashing using Akka/HTTP-based simulator.

#### Name: Carlos McNulty, Shyam Patel, Abram Gorgis, Priyan Sureshkumar
#### Net-ID: CARLOS@uic.edu SHYAM@uic.edu ABRAM@uic.edu psures5@uic.edu

#
##### Driver Langauge:          Scala 2.13.1
##### Testing Framework:        ScalaTest
##### Building Framework:       SBT
##### Configuration Library:    Typesafe
##### Other:       
                            1 simulation .conf files with name(s) XXXXXXXXXXXX.conf
#
                            
##### Main Driver Class:        Driver in Driver.scala

##### Required Libraries: 
                            "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
                            "ch.qos.logback" % "logback-classic" % "1.2.3",
                            "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
                            "org.scalatest" %% "scalatest" % "3.0.8" % Test
                            
#### To Compile: 

##### To Run Simulation(s):
                         
##### To Run Tests(in test directory): sbt test