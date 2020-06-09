package com.wise.groups

import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import com.wise.utils
import com.wise.{BasicTable, ErrorTable}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//import scala.reflect._

trait GrouperValue {
  def +(that: GrouperValue): GrouperValue
  def toJson: String
}

abstract class Grouper() extends java.io.Serializable {

  Grouper.addGroup(this)

  def getKey(call: Call): String
  def getValues(call: Call): GrouperValue
  def updateValue(rowkey: String, values: GrouperValue)

  def getPartialKey(id: Integer, date:Date): String = {
    s"${id.toString}|${utils.formatDate(date)}|"
  }

  def group( rdd: RDD[(Some[String], Option[Call], String)]) : RDD[(String, GrouperValue)] = {

    //println (s"Received ${rdd.count} rows to group ${this.getClass().getName()}")
    //val result = rdd.map {
    rdd.map {
      case (Some(key), Some(call), message: String) =>
        try {
          (this.getKey(call), this.getValues(call))
        } catch { //treating exceptions
          case e: Exception =>
            Logger.getLogger(this.getClass).error(e.getMessage)
            ErrorTable.putValues(s"Error grouping data for ${this.getClass().getName()}", call.toString)
            ("", null)
        }
      case (_,_,message) => {
        Logger.getLogger(this.getClass).error(message)
        ErrorTable.putValues(s"Strange message for group ${this.getClass().getName()}", message)
        ("", null)
      }
    }
    .filter((f) => (!f._1.isEmpty)) //removing errors
    .reduceByKey((acc,element) => acc+element) //grouping values
    //println (s"Created ${result.count} groups")
    //return result
  }

  def removeGroups(sc: SparkContext, id: Integer, begin: Date, end: Date, table: BasicTable) = {
    table.scanAndDelete(sc, getPartialKey(id, begin), getPartialKey(id, end))
  }

  def remove(sc: SparkContext, id: Integer, begin: Date, end: Date)

}

object Grouper {

  private val groups: Array[Grouper] = Array.empty[Grouper]

  def groupAndUpdateAll(status: RDD[(Some[String], Option[Call], String)]) = {
    groups.foreach(g =>
      g.group(status).foreach{case (rowkey:String, value: GrouperValue) => g.updateValue(rowkey, value)}
    )
  }

  def addGroup(g: Grouper) = {
    groups :+ g
  }

  def removeAll(sc: SparkContext, id: Integer, begin: Date, end: Date) = {

    groups.foreach(g => g.remove(sc, id, begin, end))

    //Logger.getLogger(this.getClass).setLevel(Level.INFO)
  }

  def groupAll() = {

  }

}
