package com.wise

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


case class TableData (
  col: String,
  value: Any
)

abstract class BasicTable extends java.io.Serializable {
  def name: String
  def namespace: String = "default"
  def columns: Map[String, String]  //[ColumnName, ColumnFamily]
  def table: Table = {
    HBaseConnector.getOrCreateTable(this)
  }

  implicit class TableDataByteValue(td: TableData) {
    def valueAsByte: Array[Byte] = {
      td.value.asInstanceOf[Array[Byte]]
    }
  }

  def convertToPut(rowkey: String, data: Array[TableData]): Put = {
    Logger.getLogger(this.getClass).debug(s"Converting to put ${data}")

    val put = new Put(Bytes.toBytes(rowkey))
    for (entry <- data) {
      val v = entry.value match {
        case s: String => Bytes.toBytes(s)
        case n: Int => Bytes.toBytes(n.toString)
        case l: Long => Bytes.toBytes(l.toString)
        case b: java.math.BigDecimal => Bytes.toBytes(b.toString)
        case _ => utils.serializer(entry.value)
      }
      put.addColumn(Bytes.toBytes(columns(entry.col)), Bytes.toBytes(entry.col), v)
    }
    put
  }

  def convertFromGet(result: Result): Map[String,TableData] = {
    Logger.getLogger(this.getClass).debug(s"Converting to put ${result}")

    var map = Map[String,TableData]()
    if (!result.isEmpty) {
      for ((col,cf) <- columns) {
        val value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(col))
        val data = TableData(col, value)
        map += (col -> data)
      }
    }
    map
  }

  def put(rowkey: String, data: Array[TableData]) = {
    table.put(convertToPut(rowkey, data))
  }

  def get(rowkey: String): Map[String,TableData] = {
    val get = new Get(Bytes.toBytes(rowkey))
    val result = table.get(get)
    convertFromGet(result)
  }

  def exists(rowkey: String):Boolean = {
    val get = new Get(Bytes.toBytes(rowkey))
    table.exists(get)
  }

  def scanAndDelete(sc: SparkContext, begin: String, end: String) = {
    val conf: Configuration = HBaseConnector.buildConf(name, begin, end)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map{
      case (_,result) =>
        Logger.getLogger(this.getClass).info(s"Deleting ${result}")
        table.delete(new Delete(result.getRow))
    }

    Logger.getLogger(this.getClass).info(s"Deleted ${rdd.count} rows in ${name}")

    rdd

  }

}
