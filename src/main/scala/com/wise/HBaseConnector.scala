package com.wise

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

object HBaseConnector {

    val config: Configuration = HBaseConfiguration.create
    config.set("hbase.zookeeper.quorum", "localhost");
    config.set("hbase.zookeeper.property.clientPort", "2181");
    val connection: Connection = ConnectionFactory.createConnection(config)
    val admin: Admin = connection.getAdmin

    abstract class TableProps{
      def name: String
      def cfs: Map[String, Array[Byte]]
      def columns: Map[String, Array[Byte]]
    }

    case class AccountTable(
                           name: String = "conta",
                           cfs: Map[String, Array[Byte]] = Map(
                             ("cf", Bytes.toBytes("cf"))
                           ),
                           columns: Map[String, Array[Byte]] = Map(
                             ("bank", Bytes.toBytes("bank")),
                             ("accountName", Bytes.toBytes("accountName"))
                           )
                         ) extends TableProps

  val accountTable : AccountTable = AccountTable()


  // Put row from RDD
  def putRow(data: (String, Any)): Unit = data match{
    case(key: String, acc: Account) =>
      val table = getOrCreateTable(accountTable)
      table.put(convertToPutCache(key, "bank", acc.bank.concat("|".concat(acc.accountName))))
      table.
//      table.put(convertToPutCache(key, "account", acc.accountName))

  }

  // Get table if exists, else create table
  def getOrCreateTable(table: TableProps): Table = {
    val tableName = TableName.valueOf(table.name)
    if (!admin.tableExists(tableName)) {
      println(s"Creating ${table.name} Table")
      val tableDesc = new HTableDescriptor(tableName)
      table.cfs.foreach(cf => tableDesc.addFamily(new HColumnDescriptor(cf._2)))
      admin.createTable(tableDesc)
    }
    connection.getTable(tableName)
  }

//   Converting cache rdd row to Put object
  def convertToPutCache(key: String, acc: Account) : Put = {
    val put = new Put(Bytes.toBytes(key))
    put.addColumn(accountTable.cfs("cf"), accountTable.columns("bank"), Bytes.toBytes(acc.bank))
    put
  }

  def convertToPutCache(key: String, column: String, value: String) : Put = {
    val put = new Put(Bytes.toBytes(key))
    put.addColumn(accountTable.cfs("cf"), accountTable.columns(column), Bytes.toBytes(value))
    put
  }

}
