name := "GeneratedDataBenchmark"

version := "0.1"

scalaVersion := "2.11.6"

val sparkVersion = "2.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % "2.0.1",

  "org.twitter4j" % "twitter4j-core" % "3.0.3",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3"
)
