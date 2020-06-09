package com.wise

import java.nio.ByteBuffer
import java.util.ArrayList

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream, SparkAWSCredentials}
import org.apache.spark.streaming.{Duration, StreamingContext}
import scala.util.control.Breaks._

object StreamBuilder {

  val billInvoiceStreamName: String = "bill-invoice"
  val cacheBackendStreamName: String = "cache-backend"

  def build(ssc: StreamingContext, checkpointInterval: Duration, name: String) = {
    KinesisInputDStream.builder
      .streamingContext(ssc)
      .regionName("us-east-1")
      .endpointUrl("https://kinesis.us-east-1.amazonaws.com")
      .streamName(s"$name")
      .initialPosition(new KinesisInitialPositions.TrimHorizon())
      .checkpointAppName(s"${name}stream")
      .checkpointInterval(checkpointInterval)
      .storageLevel(StorageLevel.DISK_ONLY_2)
      .build()
  }

  def buildPriority(ssc: StreamingContext, checkpointInterval: Duration) = {
    KinesisInputDStream.builder
      .streamingContext(ssc)
      .regionName("us-east-1")
      .endpointUrl("https://kinesis.us-east-1.amazonaws.com")
      .streamName("billing-teste")
      .initialPosition(new KinesisInitialPositions.TrimHorizon())
      .checkpointAppName("fernandostream2")
      .checkpointInterval(checkpointInterval)
      .dynamoDBCredentials(SparkAWSCredentials.builder.basicCredentials("", "").build())
      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
      .kinesisCredentials(SparkAWSCredentials.builder.basicCredentials("", "").build())
      .build()
  }

  def putMessage() = {
//    val credentials = new BasicAWSCredentials("AKIAVBV4NQ3L2IWTJZ6R", "1T45sTwMLoqxDmydYAhPe/JRcEAm5B2jL2+5nfsA")
    val credentials = new MyCredentals("AKIAVBV4NQ3L2IWTJZ6R", "1T45sTwMLoqxDmydYAhPe/JRcEAm5B2jL2+5nfsA")
    val clientConfiguration = new ClientConfiguration
    val clientBuilder = AmazonKinesisClientBuilder.standard()
    clientBuilder.setCredentials(credentials)
    clientBuilder.setRegion("us-east-1")
    clientBuilder.setClientConfiguration(clientConfiguration)
    val kinesisClient = clientBuilder.build()
    val putRecordsRequest =  new PutRecordsRequest
//    putRecordsRequest.setStreamName("billing-teste")
    putRecordsRequest.setStreamName("billing-fernando")

    var count = 0
    val putRecordsRequestEntryList = new ArrayList[PutRecordsRequestEntry]
    val source = scala.io.Source.fromFile("/Users/fernando/Downloads/kinesis_out.txt")
//    val lines = try source.getLines() finally source.close()
    try {
//      print(s"${source.getLines.length}")
      val lines = source.getLines()
      for (l <- lines) {
        val putRecordsRequestEntry = new PutRecordsRequestEntry
        putRecordsRequestEntry.setData(ByteBuffer.wrap(l.getBytes))
        putRecordsRequestEntry.setPartitionKey("record_".concat((count%5).toString))
        putRecordsRequestEntryList.add(putRecordsRequestEntry)
        count += 1
        if (count % 100 == 0) {
          putRecordsRequest.setRecords(putRecordsRequestEntryList)
          val putRecordsResult = kinesisClient.putRecords(putRecordsRequest)
          System.out.println("Put Result" + putRecordsResult)
          putRecordsRequestEntryList.clear()
//          break
        }
      }
      if (!putRecordsRequestEntryList.isEmpty) {
        putRecordsRequest.setRecords(putRecordsRequestEntryList)
        val putRecordsResult = kinesisClient.putRecords(putRecordsRequest)
        System.out.println("Put Result" + putRecordsResult)
        putRecordsRequestEntryList.clear()

      }
    }
      catch {
        case t:Throwable => print(s"Exception => ${t.getMessage}")
      }
    finally {
      source.close
    }



//    putRecordsRequest.setRecords(putRecordsRequestEntryList)
//    val putRecordsResult = kinesisClient.putRecords(putRecordsRequest)
//    System.out.println("Put Result" + putRecordsResult)

  }

}



class MyCredentals(accessKeyId : String, secretKey : String) extends AWSCredentialsProvider {
  override def getCredentials: AWSCredentials = {
    new BasicAWSCredentials(accessKeyId, secretKey)
  }

  override def refresh(): Unit = {}
}