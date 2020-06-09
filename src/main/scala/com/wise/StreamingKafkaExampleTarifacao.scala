package com.wise

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._



object StreamingKafkaExampleTarifacao {

  def main(args: Array[String]) = {
    new Runner3().run
  }
}


class Runner3 extends java.io.Serializable {
  def run : Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming teste")
    val conf = new SparkConf().setAppName("streaming teste")

    val streamingContext = new StreamingContext(conf, Seconds(10))
    val sc = streamingContext.sparkContext

    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "parallels-Parallels-Virtual-Platform:9092",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("teste2")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(element => {
          val rowkey = element.value()
          val basicTable: BasicTable = new BasicTable(){

            def name: String = "test"
            def columns: Map[String, String] =  Map (
              "ticket" -> "data"
            )
          }

          val data = Array[TableData](
            TableData("ticket", rowkey)
          )
          basicTable.put(rowkey, data)
        })
      }
    })


    // funciona tb
//    stream.foreachRDD(rddpartition => {
//      rddpartition.foreachPartition(rdd => {
//        if (!rdd.isEmpty) {
//          val completed: Iterator[(String, Account)] =
//            rdd.map(element => {
//              //                        System.out.println("VALOR DO ELEMENTE => " + element.value())
//              val lines = element.value().split(";").toSeq
//              val acc: Account = Account(lines(0), lines(1))
//              val rowkey = element.value()
//              (rowkey, acc)
//            })
//
//          completed.foreach(HBaseConnector.putRow)
//
//        }
//      })
//    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
