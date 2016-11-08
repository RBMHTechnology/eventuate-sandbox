organization := "com.rbmhtechnology"

name := "eventuate-sandbox"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.4.12",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.12" % "test",
  "org.scalatest"     %% "scalatest"    % "3.0.0" % "test",
  "org.scalaz"        %% "scalaz-core"  % "7.1.0"
)
