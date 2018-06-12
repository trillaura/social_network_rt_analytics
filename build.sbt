name := "social_network_rt_analytics"

version := "0.1"

scalaVersion := "2.12.6"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  DefaultMavenRepository, Resolver.sonatypeRepo("public")
)

/* don't run tests for assembly */
test in assembly := {}


libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.1"  ,
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.typesafe" % "config" % "1.3.2",
  "joda-time" % "joda-time" % "2.9.9",
  "org.joda" % "joda-convert" % "2.0.1"
)