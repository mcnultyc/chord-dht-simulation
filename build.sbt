name := "carlos_mcnulty_project"

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,

"com.typesafe.akka" %% "akka-http"   % "10.1.11",
"com.typesafe.akka" %% "akka-stream" % akkaVersion // or whatever the latest version is
)