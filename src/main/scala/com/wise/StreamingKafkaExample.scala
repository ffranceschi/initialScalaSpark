package com.wise

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

case class Account(bank: String, accountName: String)

object StreamingKafkaExample {

  def main(args: Array[String]) = {
    new Runner().run
  }
}


class Runner extends java.io.Serializable {
  def run : Unit = {
    val CONTA = "conta"
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



//    stream.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreachPartition { iter =>
//        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      }
//    }

//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set("hbase.zookeeper.quorum", "localhost");
//    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
//    val connection = ConnectionFactory.createConnection(hbaseConf)
//    val table = connection.getTable(TableName.valueOf(Bytes.toBytes(CONTA)))
//    var lines1 : Array[String] = ()
    stream.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {
        // Put example
        rdd.foreach(element => {
          val hbaseConf = HBaseConfiguration.create()
          hbaseConf.set("hbase.zookeeper.quorum", "localhost");
          hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
          val connection = ConnectionFactory.createConnection(hbaseConf)
          val table = connection.getTable(TableName.valueOf(Bytes.toBytes(CONTA)))
          System.out.println(element.value())
          val lines = element.value().split(";")
//          val acc : Account = Account(lines(0), lines(1))
          val put = new Put(Bytes.toBytes(element.value()))
//          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("bank"), Bytes.toBytes(acc.bank)
//          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("account"), Bytes.toBytes(acc.accountName))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("bank"), Bytes.toBytes(lines(0)))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("account"), Bytes.toBytes(lines(1)))
          table.put(put)
        })
      }

    })




    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
