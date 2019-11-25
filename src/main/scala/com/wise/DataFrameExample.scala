package com.wise

import org.apache.spark.sql.SparkSession

object DataFrameExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Initial Scala Spark")
      .getOrCreate()
    import spark.implicits._


    val authors = Seq("ola", "oi")
    val authorsDF = spark
      .sparkContext
      .parallelize(authors)
      .toDF()

//    authorsDF.createOrReplaceTempView("data")
//    val res = spark.sql("select count(*) from data")
//    res.write.csv("abc.txt")

    authorsDF.write.format("csv")
      .option("mode", "OVERWRITE")
      .option("dateFormat", "yyyy-MM-dd")
      .option("path", "./bbb")
      .option("sep", ";")
      .save()


  }
}