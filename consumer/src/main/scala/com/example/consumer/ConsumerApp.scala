package com.example.consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.implicits._

object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StructuredStreamingConsumer")
      .master("local[*]")
      .getOrCreate()


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "my_topic")
      .load()

    val messages = df.selectExpr("CAST(value AS STRING)").as[String]

    messages.writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}