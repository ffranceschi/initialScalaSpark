package com.wise

import org.apache.hadoop.hbase.client.{ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._


object CacheTable extends BasicTable {

  def name: String = "cache"

  val jsonKey: String = "json"

  def columns: Map[String, String] =  Map (
    //Column -> ColumnFamily
    jsonKey -> "cache_data"
  )

  def putValues(rowkey:String, json:String):Unit = {
    val data = Array[TableData](
      TableData(jsonKey, json)
    )
    this.put(rowkey, data)
  }

  def scan(): List[(String, String)] = {
    val scan: ResultScanner = table.getScanner(new Scan())
    val cacheMap = scan.asScala.map(result =>
      (Bytes.toString(result.getRow), Bytes.toString(result.getValue(Bytes.toBytes("cache_data"), Bytes.toBytes(jsonKey))))
    )
    cacheMap.toList
  }


}
