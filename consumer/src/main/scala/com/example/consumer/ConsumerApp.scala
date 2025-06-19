package com.example.consumer

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import scala.util.Try

case class StockData(
                      ticker: String,
                      volume: Long,
                      open: Double,
                      close: Double,
                      high: Double,
                      low: Double,
                      window_start: Long,
                      transactions: Long
                    )

object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StructuredStreamingConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "my_topic")
      .option("startingOffsets", "latest") // Important pour démarrer proprement
      .load()

    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    // Parsing sécurisé
    val parsedDS = messages.flatMap { line =>
      val parts = line.split(",")
      if (parts.length == 8) {
        Try {
          Some(StockData(
            parts(0),
            parts(1).toLong,
            parts(2).toDouble,
            parts(3).toDouble,
            parts(4).toDouble,
            parts(5).toDouble,
            parts(6).toLong,
            parts(7).toLong
          ))
        }.getOrElse(None)
      } else {
        None
      }
    }

    val query = parsedDS.writeStream
      .foreachBatch { (batchDF: Dataset[StockData], batchId: Long) =>
        val count = batchDF.count()
        println(s"🔥 Batch $batchId reçu avec $count lignes")

        if (count > 0) {
          try {
            batchDF.write
              .format("jdbc")
              .option("url", "jdbc:postgresql://postgres:5432/postgres")
              .option("dbtable", "public.stock_data")
              .option("user", "spark")
              .option("password", "spark123")
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save()

            println(s"✅ Batch $batchId inséré avec succès")
          } catch {
            case e: Exception =>
              println(s"❌ Erreur d'insertion JDBC dans batch $batchId : ${e.getMessage}")
              e.printStackTrace()
          }
        } else {
          println(s"⚠️ Batch $batchId vide (aucune ligne à insérer)")
        }
      }
      .start()

    query.awaitTermination()
  }
}