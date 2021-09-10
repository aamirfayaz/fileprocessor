name := "fileprocessing"

version := "0.1"

scalaVersion := "2.13.4"

val AkkaVersion = "2.6.14"

libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "3.0.3",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)