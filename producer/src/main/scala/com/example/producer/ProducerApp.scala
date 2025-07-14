package com.example.sparkcoreproducer

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ProducerApp {
  def main(args: Array[String]): Unit = {
    println("Lancement du Producer Scala avec Spark vers Kafka")

    val spark = SparkSession.builder()
      .appName("Spark Kafka Producer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Lire le CSV avec Spark
    val df_input = spark.read
      .option("header", "true")
      .csv("/data/dataset_stock/2025-04-11.csv")

    // Ajout id selon window_start
    val w = Window.orderBy($"window_start".cast("long"))
    val df_indexed = df_input
      .withColumn("row_id", row_number().over(w).cast("long") - 1)

    // Paramètres des batchs
    val batchSize   = 100L
    val totalCount  = df_indexed.count()
    val maxBatchId  = (totalCount + batchSize - 1) / batchSize  // plafond

    // Envoi des batchs
    for (batchId <- 0L until maxBatchId) {
      val start = batchId * batchSize
      val end   = start + batchSize - 1

      val batchDF = df_indexed
        .filter($"row_id".between(start, end))
        .drop("row_id")

      batchDF
        .selectExpr(
          "CAST(NULL AS STRING) AS key",
          "to_json(struct(*)) AS value"
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("topic", "my_topic")
        .save()

      println(s"Batch ${batchId} envoyé")
    }


    spark.stop()
    println("Fin de l'envoi Spark -> Kafka")
  }
}
