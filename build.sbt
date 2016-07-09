name := "meglytics"

version := "0.2"

scalaVersion := "2.11.7"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
resolvers += "Bintray scalaz" at "https://dl.bintray.com/scalaz/releases/"
resolvers += "Bintray megamsys" at "https://dl.bintray.com/megamsys/scala/"


libraryDependencies ++= Seq (
  "spark.jobserver" %% "job-server-api" % "0.6.0" % "provided",
  "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.3.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.9" % "provided",
  "io.megam" %% "libcommon" % "0.26" % "provided"
)
