package com.wise

import java.util.Date


object ErrorTable extends BasicTable {

  def name: String = "error2"

  val reasonKey: String = "reason"
  val messageKey: String = "message"

  val dataColumnFamily = "data"

  def columns: Map[String, String] =  Map (
    //Column -> ColumnFamily
    reasonKey -> dataColumnFamily,
    messageKey-> dataColumnFamily
  )

  def putValues(reason:String, message:String):Unit = {
    //val rowkey = utils.formatDate(new Date(), "yyyyMMddHHmmssSSS")

    val rowkey = utils.formatDateToTimestamp(new Date())
    val data = Array[TableData](
      TableData(reasonKey, reason),
      TableData(messageKey, message)
    )
    this.put(rowkey, data)
  }
}
