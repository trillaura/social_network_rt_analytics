name := "social_network_rt_analytics"

version := "0.1"

scalaVersion := "2.11.8"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  DefaultMavenRepository, Resolver.sonatypeRepo("public")
)

/* don't run tests for assembly */
test in assembly := {}

val flinkVersion = "1.5.0"

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.typesafe" % "config" % "1.3.2",
  "joda-time" % "joda-time" % "2.9.9",
  "org.joda" % "joda-convert" % "2.0.1")

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)

libraryDependencies ++= flinkDependencies