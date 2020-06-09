package com.wise

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

import scala.xml.XML

object HBaseConnector {

    val config: Configuration = HBaseConfiguration.create
//    val hbaseXmlFl = XML.loadFile("/etc/hbase/conf.dist/hbase-site.xml") \\ "property"  // Rodar remoto
//    val zookeeperQuorum = (hbaseXmlFl.filter(n => n.text.contains("hbase.zookeeper.quorum")) \ "value").text.trim   // Rodar remoto
//    val hbaseRootVal = (hbaseXmlFl.filter(n => n.text.contains("hbase.rootdir")) \ "value").text.trim   // Rodar remoto
//  config.set("hbase.zookeeper.quorum", zookeeperQuorum)
//  config.set("hbase.rootdir", hbaseRootVal)
    config.set("hbase.zookeeper.quorum", "localhost") // Rodar local
    config.set("hbase.zookeeper.property.clientPort", "2181")  // Rodar local
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
//  def putRow(data: (String, Any)): Unit =
//    data match {
//      case(key: String, acc: Account) =>
//        println("--------- entrou em conta")
//        val table = getOrCreateTable(accountTable)
//        table.put(convertToPutCache(key, "bank", acc.bank.concat("|".concat(acc.accountName))))
//      case(key: String, acc: String) =>
//        println("--------- entrou em string")
//        val table = getOrCreateTable(accountTable)
//        table.put(convertToPutCache(key, "bank", acc))
//  }

  // Put row from RDD
  def putRow(data: (String, Any), table: Table): Unit = data match{
    case(rowKey: String, account: Account) =>
      table.put(convertToPutAccount(rowKey, account))

    case _ =>
      Logger.getLogger(this.getClass).info(s"ERROR PUT ROW: ${data._1}:${data._2}")
  }

  def convertToPutAccount(rowKey: String, account: Account): Put = {
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(accountTable.cfs("cfAccountData"), accountTable.columns("linha"), Bytes.toBytes(account.accountName))
    put
  }

  // Get table if exists, else create table
  def getOrCreateTable(table: BasicTable): Table = {
    val tableName = TableName.valueOf(s"${table.namespace}:${table.name}")
    if (!admin.tableExists(tableName)) {
      Logger.getLogger(this.getClass).info(s"Creating ${table.name} Table")
      val tableDesc = new HTableDescriptor(tableName)
      for (cf <- table.columns.values.toList.distinct) {
        tableDesc.addFamily(new HColumnDescriptor(cf))
      }
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


  def buildConf(inputTable: String, scanRowStart: String, scanRowStop: String): Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    //conf.addResource("/etc/hbase/conf.dist/hbase-site.xml"); //172.31.43.208
    //val ip = System.getenv("SPARK_MASTER_IP")
    //    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    //    conf.set("hbase.rootdir", hbaseRootVal)
    //conf.set("hbase.zookeeper.quorum", s"${ip}:2181")
    //conf.set("hbase.rootdir", s"hdfs://${ip}:8020/user/hbase")
    conf.set(TableInputFormat.INPUT_TABLE, inputTable)
    conf.set(TableInputFormat.SCAN_ROW_START, scanRowStart)
    conf.set(TableInputFormat.SCAN_ROW_STOP, scanRowStop)


    conf
  }

  def buildConf(inputTable: String, scanRowStart: String, scanRowStop: String, scanColumns: String): Configuration = {
    val conf: Configuration = this.buildConf(inputTable, scanRowStart, scanRowStop)
    conf.set(TableInputFormat.SCAN_COLUMNS, scanColumns)

    conf
  }

}
