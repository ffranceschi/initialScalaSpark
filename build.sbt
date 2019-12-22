name := "initialScalaSpark"
organization := "com.wise"
version := "0.1"
//scalaVersion := "2.12.8"
scalaVersion := "2.11.8"

// Spark Information
val sparkVersion = "2.4.0"
val kafkaVersion = "2.2.1"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

resolvers += "MavenRepository" at
  "https://mvnrepository.com/"

libraryDependencies ++= Seq(
  // hbase
  "org.apache.hbase" % "hbase-client" % "1.4.9" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.4.9" % "provided",
  "org.apache.hbase" % "hbase-server" % "1.4.9" % "provided",

  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.12.1"

  // the rest of the file is omitted for brevity
)