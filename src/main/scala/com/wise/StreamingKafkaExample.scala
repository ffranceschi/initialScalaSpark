package com.wise

import java.io.{File, PrintWriter}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._

object StreamingKafkaExample  {

  def main(args: Array[String]) = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming teste")

    val streamingContext = new StreamingContext(conf, Seconds(10))
    val sc = streamingContext.sparkContext

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "parallels-Parallels-Virtual-Platform:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("teste1")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    stream.map(record => (record.key, record.value))

    val lines = stream.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

//    val file = new File("zzz.csv")
//    val printWriter = new PrintWriter(file)
//    printWriter.println(wordCounts.print())
//    printWriter.close()

//    stream.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreachPartition { iter =>
//        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      }
//    }

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost");
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf( Bytes.toBytes("conta") ) )

    // Put example
    var put = new Put(Bytes.toBytes("row1"))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
    table.put(put)


    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
