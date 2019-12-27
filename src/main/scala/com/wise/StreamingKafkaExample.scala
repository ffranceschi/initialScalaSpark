package com.wise

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.rdd.RDD
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

    val lines = stream.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()



    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val completed: RDD[(String, Account)] =
          rdd.map(element => {
//            System.out.println("VALOR DO ELEMENTE => " + element.value())
          val lines = element.value().split(";")
          val acc : Account = Account(lines(0), lines(1))
          val rowkey = element.value()
          (rowkey, acc)
        })
        completed.foreach(HBaseConnector.putRow)
//        completed.foreach(i => {
//          System.out.println("Valor eh")
//          System.out.println(i._1)
//          System.out.println(i._2)
//
//        })
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
