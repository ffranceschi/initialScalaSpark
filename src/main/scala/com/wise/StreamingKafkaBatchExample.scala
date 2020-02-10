package com.wise

import java.io.IOException
import java.util

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


case class Account2(val bank: String, account: String)

object StreamingKafkaBatchExample {

  def main(args: Array[String]) = {
    new Runner2().run
  }
}


class Runner2 extends java.io.Serializable {

  def hbaseWriter(iterator: Iterator[ConsumerRecord[String, String]], columnFamily: String): Unit = {
    val conf = HBaseConfiguration.create()
    var table: Table = null
    var connection: Connection = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf("cache"))
      val iteratorArray = iterator.toArray
//      val rowList = new util.ArrayList[Get]()
//      for (row <- iteratorArray) {
//        val get = new Get(Bytes.toBytes(row.key()))
//        rowList.add(get)
//      }

      // Obtain table1 data.
//      val resultDataBuffer = table.get(rowList)

      // Set the table1 data.
      val putList = new java.util.ArrayList [Put]()

//      for (i <- 0 until iteratorArray.size) {
      for (i <- iteratorArray) {
//        val row = iteratorArray(i)
//        val resultData = resultDataBuffer(i)
        if (!i.value().isEmpty) {
          // Obtain the old value based on the column family and column.//
//          val aCid = Bytes.toString(resultData.getValue(columnFamily.getBytes, "cid".getBytes))
          val put = new Put(Bytes.toBytes(i.value()))
          // Calculation result //
//          val resultValue = row.toInt + aCid.toInt
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("conta"), Bytes.toBytes(i.value()))
          putList.add(put)
        }
      }
      if (putList.size() > 0) {
        table.put(putList)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          // Close the HBase connection.//
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }

  }

  def run : Unit = {
    val confSpark = new SparkConf().setMaster("local[*]").setAppName("streaming teste")

    val streamingContext = new StreamingContext(confSpark, Seconds(10))
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
    val stream : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd =>
      rdd.foreachPartition(iterator => hbaseWriter(iterator, "cf"))
    )

//  val conf : org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()

//  val hbaseContext = new JavaHBaseContext(sc, HBaseConfiguration.create)

//  hbaseContext.streamBulkPut(stream, TableName.valueOf("cache"), HBaseConnector.convertToPutCache("0001", "conta", "0001"))

//    val confH = HBaseConnector.config
//    implicit val config: HBaseConfig = HBaseConfig(confH)




//    confH.set(TableInputFormat.INPUT_TABLE, "cache")

//    val cacheRDD = sc.newAPIHadoopRDD(confH, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map{
//      case(key, row) =>
//        Bytes.toString(row.getRow) -> Bytes.toString(row.getValue(HBaseConnector.cacheTable.cfs("cfCacheData"), HBaseConnector.cacheTable.columns("json")))
//    }
//    val streamAccount = stream.map((a,b) => separaRow(b))

    stream.map(record => (record.key, record.value))

/*
    stream.foreachRDD(rdd => {
      if(!rdd.isEmpty){
        rdd.map(separaRow).map(separaKeyAndValue)
        .toHBaseBulk("cache")
      }
    })
    */



//    stream.foreachRDD(rdd => {
//        if (!rdd.isEmpty) {
//          rdd.map(element => {
//            val lines = element.value().split(";").toSeq
//            val acc: Account = Account(lines(0), lines(1))
//            val rowkey = element.value()
//            val put = Map("cf" -> Map("conta" -> Bytes.toBytes(lines)))
//            (rowkey, put)
//          })
//        })
//
//      })


    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def separaRow(data: ConsumerRecord[String, String]): (String, String) = {
    (data.key(), data.value())
  }

  def separaKeyAndValue(t: Tuple2[String, String]): (String, Map[String, Map[String, Array[Byte]]]) = {
    val put = Map("cf" -> Map(
      "account" -> Bytes.toBytes(t._2.toString)
    ))
    (t._1, put)
  }

}
