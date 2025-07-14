ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0-SNAPSHOT"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "consumer-app",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-token-provider-kafka-0-10" % sparkVersion,
      "org.apache.kafka" % "kafka-clients" % "2.8.1",
      "org.postgresql" % "postgresql" % "42.3.1"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyJarName := "consumer-app-assembly-1.0.0-SNAPSHOT.jar"
  )

enablePlugins(sbtassembly.AssemblyPlugin)
