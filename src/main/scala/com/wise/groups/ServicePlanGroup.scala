package com.wise.groups

import java.math.RoundingMode
import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import com.wise.model.ServicePlanGroupTable
import com.wise.utils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


case class ServicePlanValues(
    id:Int,
    duration:Int,
    cost:java.math.BigDecimal,
    quantity:Int=1,
    servicePlan:Int,
    referenceDate:String
  ) extends GrouperValue{
  def +(that: GrouperValue): ServicePlanValues = {
    val v = that.asInstanceOf[ServicePlanValues]
    ServicePlanValues(
      id,
      this.duration + v.duration,
      this.cost.add(v.cost),
      this.quantity + v.quantity,
      this.servicePlan,
      this.referenceDate
    )
  }
  def toJson: String = {
    s"""{ "clientId":$id, "duration":$duration, "cost":${cost.setScale(4, RoundingMode.CEILING)}, "quantity":$quantity, "servicePlan":$servicePlan, "referenceDate":"$referenceDate"}"""
  }
}

object ServicePlanGroup extends Grouper() {

  def getKey(call: Call): String = {
    getPartialKey(call.getCenterCollect.getIdClient, call.getDate) +
    s"${call.getContractBillingDetail.getServicePlan.getId}"
  }

  def getValues(call: Call): ServicePlanValues = {
    val id = call.getCenterCollect.getIdClient
    val duration = call.getDuration
    val cost = call.getCost
    val quantity = 1
    val servicePlan = call.getContractBillingDetail.getServicePlan.getId
    val referenceDate = utils.formatDate(call.getDate)

    ServicePlanValues( id, duration, cost, quantity, servicePlan, referenceDate)
  }

  def updateValue(rowkey: String, v: GrouperValue) = {
    Logger.getLogger(this.getClass).debug(s"Updating ServicePlanGroup values with rowKey ${rowkey} and ${v}")
    val values = v.asInstanceOf[ServicePlanValues]

    if (ServicePlanGroupTable.exists(rowkey)) {
      val oldRow = ServicePlanGroupTable.getValues(rowkey)
      val newRow = oldRow+values
      ServicePlanGroupTable.putValues(rowkey, newRow)
    } else {
        ServicePlanGroupTable.putValues(rowkey, values)
    }
  }

  def remove(sc: SparkContext, id: Integer, begin: Date, end: Date) = {
    Logger.getLogger(this.getClass).info(s"Deleting ServicePlan groups for client ${id} with date between ${begin} and ${end}")

    removeGroups(sc, id, begin, end, ServicePlanGroupTable)
  }

}
