package com.wise.groups

import java.math.RoundingMode
import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import com.wise.model.CenterGroupTable
import com.wise.utils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


case class CenterValues(
    id:Int,
    duration:Int,
    cost:java.math.BigDecimal,
    quantity:Int=1,
    center:Int,
    referenceDate:String
  ) extends GrouperValue{
  def +(that: GrouperValue): CenterValues = {
    val v = that.asInstanceOf[CenterValues]
    CenterValues(
      id,
      this.duration + v.duration,
      this.cost.add(v.cost),
      this.quantity + v.quantity,
      this.center,
      this.referenceDate
    )
  }

  def toJson: String = {
    s"""{ "clientId":$id, "duration":$duration, "cost":${cost.setScale(4, RoundingMode.CEILING)}, "quantity":$quantity, "center":$center, "referenceDate":"$referenceDate"}"""
  }

}

object CenterGroup extends Grouper() {

  def getKey(call: Call): String = {
    getPartialKey(call.getCenterCollect.getIdClient, call.getDate) +
    s"${call.getCenter.getId}"
  }

  def getValues(call: Call): CenterValues = {
    val id = call.getCenterCollect.getIdClient
    val duration = call.getDuration
    val cost = call.getCost
    val quantity = 1
    val center = call.getCenter.getId
    val referenceDate = utils.formatDate(call.getDate)

    CenterValues( id, duration, cost, quantity, center, referenceDate)
  }

  def updateValue(rowkey: String, v: GrouperValue) = {
    Logger.getLogger(this.getClass).debug(s"Updating CenterGroup values with rowKey ${rowkey} and ${v}")
    val values = v.asInstanceOf[CenterValues]

    if (CenterGroupTable.exists(rowkey)) {
      val oldRow = CenterGroupTable.getValues(rowkey)
      val newRow = oldRow+values
      CenterGroupTable.putValues(rowkey, newRow)
    } else {
        CenterGroupTable.putValues(rowkey, values)
    }
    Logger.getLogger(this.getClass).debug("Updating done")
  }

  def remove(sc: SparkContext, id: Integer, begin: Date, end: Date) = {
    Logger.getLogger(this.getClass).info(s"Deleting Center groups for client ${id} with date between ${begin} and ${end}")

    removeGroups(sc, id, begin, end, CenterGroupTable)
  }


}
