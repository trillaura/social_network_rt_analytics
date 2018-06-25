ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "it.uniroma2.sabd"

//scalaVersion := "2.11.8"


/* don't run tests for assembly */
test in assembly := {}

val flinkVersion = "1.5.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)


lazy val social_rt_analytics = (project in file("."))
    .settings(
      name := "social_network_rt_analytics",

      version := "0.1",

      //scalaVersion := "2.11.8",


      resolvers ++= Seq(
        "apache-snapshots" at "http://repository.apache.org/snapshots/",
        DefaultMavenRepository, Resolver.sonatypeRepo("public")
      ),

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
      "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion
    ),

    libraryDependencies ++= flinkDependencies
)