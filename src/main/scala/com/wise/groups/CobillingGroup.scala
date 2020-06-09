package com.wise.groups

//import br.net.mindlabs.model.Grouper
import java.math.RoundingMode
import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import com.wise.model.CobillingGroupTable
import com.wise.utils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


case class CobillingValues(
    id:Int,
    duration:Int,
    cost:java.math.BigDecimal,
    quantity:Int=1,
    cobilling:Boolean,
    defaultContract:Boolean,
    referenceDate: String
  ) extends GrouperValue{
  def +(that: GrouperValue): CobillingValues = {
    val v = that.asInstanceOf[CobillingValues]
    CobillingValues(
      id,
      this.duration + v.duration,
      this.cost.add(v.cost),
      this.quantity + v.quantity,
      this.cobilling,
      this.defaultContract,
      this.referenceDate
    )
  }
  def toJson: String = {
    s"""{ "clientId":$id, "duration":$duration, "cost":${cost.setScale(4, RoundingMode.CEILING)}, "quantity":$quantity, "cobilling":$cobilling, "defaultContract":$defaultContract, "referenceDate":"$referenceDate"}"""
  }

}

object CobillingGroup extends Grouper() {

  def getKey(call: Call): String = {
    getPartialKey(call.getCenterCollect.getIdClient, call.getDate) +
    s"${call.isCoobilling}|" +
    s"${call.isUseDefaultContract}"
  }

  def getValues(call: Call): CobillingValues = {
    val id = call.getCenterCollect.getIdClient
    val duration = call.getDuration
    val cost = call.getCost
    val quantity = 1
    val cobilling = call.isCoobilling
    val defaultContract = call.isUseDefaultContract
    val referenceDate = utils.formatDate(call.getDate)

    CobillingValues( id, duration, cost, quantity, cobilling, defaultContract, referenceDate)
  }

  def updateValue(rowkey: String, v: GrouperValue) = {
    Logger.getLogger(this.getClass).debug(s"Updating CobillingGroup values with rowKey ${rowkey} and ${v}")
    val values = v.asInstanceOf[CobillingValues]

    if (CobillingGroupTable.exists(rowkey)) {
      val oldRow = CobillingGroupTable.getValues(rowkey)
      val newRow = oldRow+values
      CobillingGroupTable.putValues(rowkey, newRow)
    } else {
        CobillingGroupTable.putValues(rowkey, values)
    }
  }

  def remove(sc: SparkContext, id: Integer, begin: Date, end: Date) = {
    //Logger.getLogger(this.getClass).setLevel(Level.INFO)
    Logger.getLogger(this.getClass).info(s"Deleting Cobilling groups for client ${id} with date between ${begin} and ${end}")

    removeGroups(sc, id, begin, end, CobillingGroupTable)
  }



}
