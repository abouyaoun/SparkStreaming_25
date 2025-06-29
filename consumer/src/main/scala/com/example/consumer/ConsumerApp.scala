package com.example.consumer

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.types._

object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val schema = new StructType()
      .add("ticker", StringType)
      .add("volume", LongType)
      .add("open", DoubleType)
      .add("close", DoubleType)
      .add("high", DoubleType)
      .add("low", DoubleType)
      .add("window_start", LongType)
      .add("transactions", LongType)

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

    // Parsing sécurisé
    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    val parsedDF = messages
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .as[StockData]


    val query = parsedDF.writeStream
      .foreachBatch { (batchDF: Dataset[StockData], batchId: Long) =>
        val rowCount = batchDF.count()
        println(s"🔥 Batch $batchId reçu avec $rowCount lignes")

        if (rowCount > 0) {
          try {
            // Agrégations Spark
            val aggDF = batchDF
              .withColumn("prix_pondere", $"close" * $"volume")
              .withColumn("volatilite", (($"high" - $"low") / $"open") * 100)
              .withColumn("roi_simule", (($"close" - $"open") / $"open") * 100)
              .groupBy($"ticker")
              .agg(
                count(lit(1)).as("nb_enregistrements"),
                avg($"volume").as("volume_moyen"),
                max($"high").as("plus_haut"),
                min($"low").as("plus_bas"),
                sum($"prix_pondere").as("somme_close_volume"),
                sum($"volume").as("somme_volume"),
                avg($"volatilite").as("volatilite_pct"),
                avg($"roi_simule").as("roi_simule_pct"),
                sum($"transactions").as("transactions_totales")
              )
              .withColumn("vwap", $"somme_close_volume" / $"somme_volume")
              .drop("somme_close_volume", "somme_volume")
              .withColumn("batch_id", lit(batchId))
              .withColumn("date_calc", current_timestamp())

            // Écriture des agrégations dans PostgreSQL
            aggDF.write
              .format("jdbc")
              .option("url", "jdbc:postgresql://postgres:5432/postgres")
              .option("dbtable", "public.stock_data_agg")
              .option("user", "spark")
              .option("password", "spark123")
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save()

            println(s"📊 Agrégations du batch $batchId insérées dans stock_data_agg ✅")

            // Écriture des données brutes
            batchDF.write
              .format("jdbc")
              .option("url", "jdbc:postgresql://postgres:5432/postgres")
              .option("dbtable", "public.stock_data")
              .option("user", "spark")
              .option("password", "spark123")
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save()

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