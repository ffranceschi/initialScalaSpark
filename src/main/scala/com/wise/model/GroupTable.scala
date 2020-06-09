package com.wise.model

import com.wise.{BasicTable, TableData}
import com.wise.groups.GrouperValue
import com.wise.utils._

abstract class GroupTable[T <: GrouperValue] extends BasicTable {

  val objKey: String = "obj"
  val jsonKey:String = "json"

  val dataColumnFamily: String = "data"

  def columns: Map[String, String] =  Map (
    //Column -> ColumnFamily
    objKey -> dataColumnFamily,
    jsonKey-> dataColumnFamily
  )

  def putValues(rowkey:String, value:T):Unit = {
    val data = Array[TableData](
      TableData(objKey, value),
      TableData(jsonKey, value.toJson)
    )
    this.put(rowkey, data)
  }

  def getValues(rowkey:String):T ={
    val data: Map[String,TableData] = super.get(rowkey)
    deserializer[T](data(objKey).valueAsByte)
  }

}
