package com.wise.groups

import java.math.RoundingMode
import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import com.wise.model.SupplierGroupTable
import com.wise.utils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


case class SupplierValues(
    id:Int,
    cost:java.math.BigDecimal,
    supplier:Int,
    referenceDate:String
  ) extends GrouperValue{
  def +(that: GrouperValue): SupplierValues = {
    val v = that.asInstanceOf[SupplierValues]
    SupplierValues(
      id,
      this.cost.add(v.cost),
      this.supplier,
      this.referenceDate
    )
  }
  def toJson: String = {
    s"""{ "clientId":$id, "cost":${cost.setScale(4, RoundingMode.CEILING)}, "supplier":$supplier, "referenceDate":"$referenceDate"}"""
  }
}

object SupplierGroup extends Grouper() {

  def getKey(call: Call): String = {
    getPartialKey(call.getCenterCollect.getIdClient, call.getDate) +
    s"${call.getContract.getSupplier.getId}"
  }

  def getValues(call: Call): SupplierValues = {
    val id = call.getCenterCollect.getIdClient
    val cost = call.getCost
    val servicePlan = call.getContractBillingDetail.getServicePlan.getId
    val referenceDate = utils.formatDate(call.getDate)

    SupplierValues( id, cost, servicePlan, referenceDate)
  }

  def updateValue(rowkey: String, v: GrouperValue) = {
    Logger.getLogger(this.getClass).debug(s"Updating SupplierGroup values with rowKey ${rowkey} and ${v}")
    val values = v.asInstanceOf[SupplierValues]

    if (SupplierGroupTable.exists(rowkey)) {
      val oldRow = SupplierGroupTable.getValues(rowkey)
      val newRow = oldRow+values
      SupplierGroupTable.putValues(rowkey, newRow)
    } else {
        SupplierGroupTable.putValues(rowkey, values)
    }
  }

  def remove(sc: SparkContext, id: Integer, begin: Date, end: Date) = {
    Logger.getLogger(this.getClass).info(s"Deleting Supplier groups for client ${id} with date between ${begin} and ${end}")

    removeGroups(sc, id, begin, end, SupplierGroupTable)
  }

}
