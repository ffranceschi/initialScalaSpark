package com.wise

import br.com.sumus.papaleguas.billing.billingcore.service.BillingServiceImpl
import org.apache.spark._
import org.apache.spark.streaming._



object StreamingKinesisExampleTarifacao {

  def main(args: Array[String]) = {
    new Runner5().run
  }
}


class Runner5 extends java.io.Serializable {
  def run : Unit = {
    val conf = new SparkConf().setAppName("streaming kinesis teste")

    val interval = Duration(5000)
    val ssc = new StreamingContext(conf, interval)

    val kinesisStream = StreamBuilder.buildPriority(ssc, interval)
    val stream = kinesisStream.map(MessageDecoder.decodeKinesisStream)

//    MessageDecoder.runProcessor(stream)


    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(element => {
          val rowkey = new String(element)
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



    ssc.start()
    ssc.awaitTermination()
  }

}
