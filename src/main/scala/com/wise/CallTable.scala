package com.wise

import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object CallTable extends BasicTable {

  def name: String = "call2"

  val callKey: String = "call"
  val ticketKey: String = "ticket"
  val valueString: String = "valueString"
  val dataColumnFamily: String = "data"

  def getKey(call: Call): String = {
    s"${call.getCenterCollect.getIdClient.toString}|" +
    s"${utils.formatDate(call.getDate)}|" +
    s"${call.getCenterOrigin.getId.toString}|" +
    s"${call.getExtension}|" +
    s"${call.getPassword}|" +
    s"${call.getCenterCollect.getIdCompany.toString}|" +
    s"${call.getCenterCollect.getId.toString}|" +
    s"${utils.formatDate(call.getHour, "HHmmss")}|" +
    s"${call.getPrimitiveNumber}|" +
    s"${call.getRouteEntry}|" +
    s"${call.getRouteExit}|" +
    s"${call.getTrunkEntry}|" +
    s"${call.getTrunkExit}|" +
    s"${call.getDuration.toString}"
  }

  def columns: Map[String, String] =  Map (
    //Column -> ColumnFamily
    callKey ->dataColumnFamily,
    valueString -> dataColumnFamily,
    ticketKey ->dataColumnFamily
  )

  def putValues(rowkey:String, call:Call, ticket:String):Unit = {
    val data = Array[TableData](
      TableData(callKey, call),
      TableData(valueString, call.toString),
      TableData(ticketKey, ticket)
    )
    this.put(rowkey, data)
  }

  def getTickets(sc: SparkContext, clientId: Integer, beginDate: Date, endDate: Date): RDD[String] = {

    val begin: String = s"${clientId.toString}|${utils.formatDate(beginDate)}"
    val end: String  = s"${clientId.toString}|${utils.formatDate(endDate)}"

    val cfamily = Bytes.toBytes(columns(ticketKey))
    val column = Bytes.toBytes(ticketKey)

    val conf: Configuration = HBaseConnector.buildConf(name, begin, end, s"${columns(ticketKey)}:${ticketKey}")

    sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map{
      case (_,result) => Bytes.toString(result.getValue(cfamily, column))
    }
  }
}
