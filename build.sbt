name := "initialScalaSpark"
organization := "com.wise"
version := "0.1"
//scalaVersion := "2.12.8"
scalaVersion := "2.11.8"

// Spark Information
val sparkVersion = "2.4.0"
val kafkaVersion = "2.2.1"
val hbaseVersion = "2.2.1"

resolvers ++= Seq(
  "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release",
  "MavenRepository" at "https://mvnrepository.com/",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/"
)

libraryDependencies ++= Seq(
  // hbase
  "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-spark" % "2.1.0-cdh6.3.3" % "provided",
//  "org.apache.hadoop.hbase.spark.HBaseContext" % "hbase-spark" % hbaseVersion % "provided",
  "eu.unicredit" %% "hbase-rdd" % "0.9.0",

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