name := "KinesisProducerLibrary"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.7.2"
val json4sVersion = "3.4.2"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.307",
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.apache.flink" %% "flink-connector-kinesis" % flinkVersion
)
