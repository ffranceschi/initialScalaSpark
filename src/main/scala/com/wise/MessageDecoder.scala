package com.wise

import br.com.sumus.papaleguas.billing.billingcore.message.dto.TicketDomain
import br.com.sumus.papaleguas.billing.billingcore.model.Call
import br.com.sumus.papaleguas.billing.billingcore.service.BillingServiceImpl
import com.wise.groups.{CenterGroup, CobillingGroup, GrouperValue, IdentifierGroup, ServicePlanGroup, SupplierGroup}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream

object MessageDecoder {

  val CACHETAG = "CACHE:"
  val TICKETTAG = "TICKET:"
  val CONNECTION_URL = "cache-teste-ro.tkiprf.ng.0001.use1.cache.amazonaws.com"
//  val CONNECTION_URL = "localhost"

  /** Decode message from kinesis
    *
    * @param data kinesis bytearray message
    * @return kinesis string message
    */

  def decodeKinesisStream(data: Array[Byte]): String = {
    (data.map(_.toChar)).mkString.stripMargin
  }

  def generateErrorStream(message: String): Boolean = {
    !message.startsWith(MessageDecoder.CACHETAG) && !message.startsWith(MessageDecoder.TICKETTAG)
  }

  def generateCacheStream(message: String): Boolean = {
    message.startsWith(MessageDecoder.CACHETAG)
  }

  def generateCallStream(message: String): Boolean = {
    message.startsWith(MessageDecoder.TICKETTAG)
  }


  def runProcessor(processorCallStream: DStream[String]) = {
    //Logger.getLogger(this.getClass).setLevel(Level.DEBUG)
    Logger.getLogger(this.getClass).info("Starting processor")
    // Parsing messages from call type

    Logger.getLogger(this.getClass).info("--- Processing Loop ---")
    Logger.getLogger(this.getClass).debug("Starting call stream processor")
    val calls = processorCallStream.map(MessageDecoder.ticketDecode).transform(rdd => {
      rdd.map {
        MessageDecoder.ticketParseNew
      }
    }).cache
    Logger.getLogger(this.getClass).debug("Finished call stream processor")

    //saving call data to history
    Logger.getLogger(this.getClass).debug("Starting saving call data to history")
    calls.foreachRDD(rdd => {
      rdd.foreach {
        MessageDecoder.saveTicketCalls
      }
    })


    Logger.getLogger(this.getClass).debug("Starting filtering ticket calls.")
    val status1 = calls.filter {
      MessageDecoder.filterTicketCalls
    }.cache

    Logger.getLogger(this.getClass).info("Processing Service Plan")
/*

    status1.foreachRDD(rdd => {
      ServicePlanGroup.group(rdd).foreach{
        case (rowkey:String, value: GrouperValue) => ServicePlanGroup.updateValue(rowkey, value)
      }
      SupplierGroup.group(rdd).foreach{
        case (rowkey:String, value: GrouperValue) => SupplierGroup.updateValue(rowkey, value)
      }
      CenterGroup.group(rdd).foreach{
        case (rowkey:String, value: GrouperValue) => CenterGroup.updateValue(rowkey, value)
      }
      CobillingGroup.group(rdd).foreach{
        case (rowkey:String, value: GrouperValue) => CobillingGroup.updateValue(rowkey, value)
      }
      IdentifierGroup.group(rdd).foreach{
        case (rowkey:String, value: GrouperValue) => IdentifierGroup.updateValue(rowkey, value)
      }
    })
*/


  }

  // Decode call message from kinesis
  def ticketDecode(data: String): (Option[TicketDomain], String) = {
    Logger.getLogger(this.getClass).debug(s"Starting ticket decode ${data}")
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

      Logger.getLogger(this.getClass).debug("Finished ticket decode successfully")
      (Some(ticket), data)
    } catch {
      case e: Exception =>
        Logger.getLogger(this.getClass).error(e.getMessage)

        (None, data)
    }
  }

  // Parse decoded tickets from kinesis stream to calls
  def ticketParseAll(decodedTicket: (Option[TicketDomain], String)): (Some[String], Option[Call], String) = {
    decodedTicket match {
      case (Some(ticket: TicketDomain), message: String) => {
        try {
          Logger.getLogger(this.getClass).debug(s"Parsing ticket ${ticket}")

          //val cacheMap = cacheBroadcast.value
          // val call = (new BillingServiceImpl).billing(ticket, cacheMap.asJava, null)
          val call = (new BillingServiceImpl).billing(ticket, null, CONNECTION_URL)
          if (call != null) {
            val rowKey = CallTable.getKey(call)
            (Some(rowKey), Some(call), message)
          } else {
            (Some("Null Call"), None, message)
          }
        } catch {
          case e: Exception =>
            Logger.getLogger(this.getClass).error(s"Falha ao criar o objeto call para $message")
            Logger.getLogger(this.getClass).error(e.getMessage)
            (Some("Call decoding fail"), None, message)
        }
      }
      case (None, message: String) => {
        Logger.getLogger(this.getClass).error(s"Erro de conversão do ticket: ${message}")
        (Some("Ticket decoding fail"), None, message)
      }
    }
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
        Logger.getLogger(this.getClass).debug(s"Parsing possible new ticket ${ticket}")
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
            Logger.getLogger(this.getClass).info(s"Falha ao criar o objeto call para $message")
            Logger.getLogger(this.getClass).error(e.getMessage)
            (Some("Call decoding fail"), None, message)
        }
      }
      case (None, message: String) => {
        Logger.getLogger(this.getClass).info(s"Erro de conversão do ticket: ${message}")
        (Some("Ticket decoding fail"), None, message)
      }
    }
  }

  def filterTicketCalls(parsedTicket: (Some[String], Option[Call], String)): Boolean = {
    // Only calls with status == 1 proceed to group
    parsedTicket match {
      case (Some(rowKey: String), Some(call: Call), message: String) => {
        try {
          (call.getStatus.getId == 1)
        } catch {
          case e: Exception =>
            Logger.getLogger(this.getClass).error(e.getMessage)
            ErrorTable.putValues("Failled to get Call status", message)
            false
        }
      }
      case (_, _, message: String) => {
        Logger.getLogger(this.getClass).warn("#### value not know")
        false
      }
    }
  }
}
