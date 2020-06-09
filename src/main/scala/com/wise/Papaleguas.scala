package com.wise

import br.com.sumus.papaleguas.billing.billingcore.message.dto.TicketDomain
import br.com.sumus.papaleguas.billing.billingcore.model.Call
import br.com.sumus.papaleguas.billing.billingcore.service.BillingServiceImpl
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._



object Papaleguas {

  def main(args: Array[String]) = {
    new Runner4().run
  }
}


class Runner4 extends java.io.Serializable {

  val TICKETTAG = "TICKET:"
  val CONNECTION_URL = "localhost"

  def run : Unit = {
    val conf = new SparkConf().setAppName("papaleguas")

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

    val topics = Array("billing-teste")
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

    val calls = stream.map(e => ticketDecode(e.value())).transform(rdd => {
      rdd.map {
        ticketParseNew
      }
    }).cache


    calls.foreachRDD(rdd => {
      rdd.foreach {
        saveTicketCalls
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def saveTicketCalls(parsedTicket: (Option[String], Option[Call], String)) = {
    parsedTicket match {
      case (Some(rowKey: String), Some(call: Call), message: String) => {CallTable.putValues(rowKey, call, message)}
      case (Some(reason: String), None, message: String) => ErrorTable.putValues(reason, message)
      case (_, _, message: String) => ErrorTable.putValues("Unknown error", message)
    }
  }

  def ticketParseNew(decodedTicket: (Option[TicketDomain], String)) = {
    decodedTicket match {
      case (Some(ticket: TicketDomain), message: String) => {
        try {
          val call = (new BillingServiceImpl).billing(ticket, null, CONNECTION_URL)
          if (call != null) {

            //val r = Random
            //r.setSeed(System.currentTimeMillis())

            val rowKey = CallTable.getKey(call) //+ s"|${r.nextInt(1000000)}"
            if (CallTable.exists(rowKey)) {
              (Some("Call already exists"), None, message)
            } else {
              //CallTable.putValues(rowKey, call, message)
              (Some(rowKey), Some(call), message)
            }
          } else {
            (Some("Null Call"), None, message)
          }
        } catch {
          case e: Exception =>
            (Some("Call decoding fail"), None, message)
        }
      }
      case (None, message: String) => {
        (Some("Ticket decoding fail"), None, message)
      }
    }
  }
  // Decode call message from kinesis
  def ticketDecode(data: String): (Option[TicketDomain], String) = {
    try {
      val strData = data.replaceFirst(TICKETTAG, "")
      val Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21) = strData.trim().split(';')

      val ticket = new TicketDomain
      ticket.setFluxo(s0)
      ticket.setData(s1)
      ticket.setHora_inicio(s2)
      ticket.setDuracao(s3)
      ticket.setNumero_ramal(s4)
      ticket.setNumero_origem(s5)
      ticket.setNumero_discado(s6)
      ticket.setRota_entrada(s7)
      ticket.setLinha_entrada(s8)
      ticket.setRota_saida(s9)
      ticket.setLinha_saida(s10)
      ticket.setSenha(s11)
      ticket.setProjeto(s12)
      ticket.setMcdu(s13)
      ticket.setBytes(s14)
      ticket.setRing(s15)
      ticket.setVdn(s16)
      ticket.setCentral_interno(s17)
      ticket.setCall_manager_conference(s18)
      ticket.setNr_call_manager_key_dest(s19)
      ticket.setNr_call_manager_key_orig(s20)
      ticket.setId_center(s21)

      (Some(ticket), data)
    } catch {
      case e: Exception =>
        //        ErrorTable.putValues(message,data)
        (None, data)
    }
  }

}
