version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.0"

initialize := {
  val _ = initialize.value // run the previous initialization
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % "10.1.11",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.11",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

dockerBaseImage := "openjdk:8"
dockerExposedPorts := Seq(8080)
enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)