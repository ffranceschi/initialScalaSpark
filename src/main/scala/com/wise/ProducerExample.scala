package com.wise

import java.util.Properties
import org.apache.kafka.clients.producer._


object ProducerExample {

  def main(args: Array[String]): Unit = {
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("client.id", "ProducerExample")
    producerProps.put("acks", "all")
    producerProps.put("retries", new Integer(1))
    producerProps.put("batch.size", new Integer(16384))
    producerProps.put("linger.ms", new Integer(1))
    producerProps.put("buffer.memory", new Integer(133554432))

    val producer = new KafkaProducer[String, String](producerProps)

    for (a <- 1 to 20000) {
      val record: ProducerRecord[String, String] = new ProducerRecord("teste1", "Hello" + a)
      producer.send(record);
    }

    producer.close()
  }

}
