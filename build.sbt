import sbt.Keys.libraryDependencies

ThisBuild / scalaVersion := "2.11.8"

ThisBuild / resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  DefaultMavenRepository, Resolver.sonatypeRepo("public"),
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "social_network_rt_analytics"
version := "0.1.0"

// organization := "it.uniroma2.sabd"

/* don't run tests for assembly */
assembly / test := {}
assembly / assemblyJarName := "app.jar"
assembly / mainClass := Some("WordCount")

val flinkVersion = "1.5.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)


lazy val social_rt_analytics = (project in file("."))
  .settings(

    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.0.1",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.typesafe" % "config" % "1.3.2",
      "joda-time" % "joda-time" % "2.9.9",
      "org.joda" % "joda-convert" % "2.0.1",
      "org.apache.kafka" % "kafka-streams" % "1.1.0",
      "org.apache.kafka" % "kafka-clients" % "1.1.0",
      "org.apache.kafka" % "kafka_2.11" % "1.1.0",
      "com.101tec" % "zkclient" % "0.9",
      "org.apache.avro" % "avro" % "1.8.2",
      "com.twitter" % "bijection-avro_2.10" % "0.9.2",
      "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion,
      "com.lightbend" %% "kafka-streams-scala" % "0.2.1",
      "com.github.cb372" %% "scalacache-guava" % "0.24.2",
      "net.debasishg" %% "redisclient" % "3.7",
      "com.google.code.gson" % "gson" % "2.8.5"

    ),

    libraryDependencies ++= flinkDependencies,
    libraryDependencies += "org.apache.storm" % "storm-core" % "1.2.2" exclude("junit", "junit")
  )


scalacOptions ++= Seq("-feature", "-deprecation", "-Yresolve-term-conflict:package")

// When doing sbt run, fork a separate process.  This is apparently needed by storm.
fork := true

resolvers += "clojars" at "https://clojars.org/repo"

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)