package com.example.consumer

import org.apache.spark.sql.{SparkSession, Dataset}
import com.example.consumer.model.StockData
import com.example.consumer.utils.MessageParser
import com.example.consumer.processing.BatchProcessor

object ConsumerApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StructuredStreamingConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "my_topic")
      .option("startingOffsets", "latest")
      .load()

    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    val parsedDS: Dataset[StockData] = messages.flatMap(MessageParser.parseLine(_).toSeq)

    val query = parsedDS.writeStream
      .foreachBatch { (batchDF: Dataset[StockData], batchId: Long) =>
        BatchProcessor.processBatch(batchDF, batchId)(spark)
      }
      .start()

    query.awaitTermination()
  }
}