package com.wise

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingExample {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming teste")

    val ssc = new StreamingContext(conf, Seconds(10))
    val textDStream = ssc.socketTextStream("localhost", 9876)
    print(textDStream.foreachRDD(a => print(a)))
    ssc.start()
    ssc.awaitTermination()

  }

}
