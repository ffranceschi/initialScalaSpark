# initialScalaSpark
Projeto basico Scala com Spark

spark-submit --class com.wise.StreamingKafkaExample --master local --packages "org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0,org.apache.spark:spark-core_2.11:2.4.0,org.apache.hbase:hbase-common:1.4.9,org.apache.hbase:hbase-client:1.4.9" target/scala-2.11/initialscalaspark_2.11-0.1.jar 

bin/flume-ng agent -n agent1 -c conf -f /Users/fernando/Documents/workspace_scala/initialScalaSpark/src/main/resources/flume.kafka