package com.wise.groups

import java.math.RoundingMode
import java.util.Date

import br.com.sumus.papaleguas.billing.billingcore.model.Call
import com.wise.model.IdentifierGroupTable
import com.wise.utils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

case class IdentifierValues(
                             id:Int,
                             duration:Int,
                             cost:java.math.BigDecimal,
                             quantity:Int=1,
                             date: String,
                            id_central: Int,
                            val_type: String,
                            identifier: String,
                            service_plan: Int
                          ) extends GrouperValue{
  def +(that: GrouperValue): IdentifierValues = {
    val v = that.asInstanceOf[IdentifierValues]
    IdentifierValues(
      id,
      this.duration + v.duration,
      this.cost.add(v.cost),
      this.quantity + v.quantity,
      this.date,
      this.id_central,
      this.val_type,
      this.identifier,
      this.service_plan
    )
  }

  override def toJson: String = {
    s"""{clientId: "$id", duration:"$duration", cost:"${cost.setScale(4, RoundingMode.CEILING)}", quantity:"$quantity", date: "$date", idCentral: "$id_central", valType: "$val_type", identifier:"$identifier", servicePlan: "$service_plan" }"""
  }

}

object IdentifierGroup extends Grouper() {

  private val serialVersionUID = 1L

  override def getKey(call: Call): String = {
    val hasPass: Boolean = (call.getPassword != null && !call.getPassword.isEmpty)
    val callPassType = if (hasPass) "PASSWORD" else "EXTENSION"
    val callIdentifier = if (hasPass) call.getPassword else call.getExtension

    getPartialKey(call.getCenterCollect.getIdClient, call.getDate) +
    s"${call.getCenterOrigin.getId}|${callPassType}|${callIdentifier}|${call.getContractBillingDetail.getServicePlan.getId}"
  }

  override def getValues(call: Call): IdentifierValues = {
    val id = call.getCenterCollect.getIdClient
    val date = utils.formatDate(call.getDate)
    val centerOrigin = call.getCenterOrigin.getId
    val hasPass: Boolean = (call.getPassword != null && !call.getPassword.isEmpty)
    val callPassType = if (hasPass) "PASSWORD" else "EXTENSION"
    val callIdentifier = if (hasPass) call.getPassword else call.getExtension
    val servicePlan = call.getContractBillingDetail.getServicePlan.getId
    val duration = call.getDuration
    val cost = call.getCost
    val quantity = 1

    IdentifierValues(id, duration, cost, quantity, date, centerOrigin, callPassType, callIdentifier, servicePlan)
  }

  override def updateValue(rowkey: String, v: GrouperValue): Unit = {
    Logger.getLogger(this.getClass).debug(s"Updating Identifier values with rowKey ${rowkey} and ${v}")
    val values = v.asInstanceOf[IdentifierValues]

    if (IdentifierGroupTable.exists(rowkey)) {
      val oldRow = IdentifierGroupTable.getValues(rowkey)
      val newRow = oldRow+values
      IdentifierGroupTable.putValues(rowkey, newRow)
    } else {
      IdentifierGroupTable.putValues(rowkey, values)
    }
  }

  override def remove(sc: SparkContext, id: Integer, begin: Date, end: Date): Unit = {
    Logger.getLogger(this.getClass).info(s"Deleting Identifier groups for client ${id} with date between ${begin} and ${end}")

    removeGroups(sc, id, begin, end, IdentifierGroupTable)
  }
}
