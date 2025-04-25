ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "producer-app",
    // Configuration pour le fat jar
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "2.8.1",
  "org.apache.kafka" % "kafka-clients" % "2.8.1"
)
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.11"

// Ajouter le plugin d'assembly
enablePlugins(sbtassembly.AssemblyPlugin)