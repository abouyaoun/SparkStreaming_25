package com.example.consumer

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import scala.util.Try

object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val dbUrl = sys.env.getOrElse("DB_URL", "jdbc:postgresql://localhost:5432/postgres")
    val dbUser = sys.env.getOrElse("DB_USER", "default_user")
    val dbPassword = sys.env.getOrElse("DB_PASSWORD", "default_password")

    val spark = SparkSession.builder
      .appName("StructuredStreamingConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "my_topic")
      .option("startingOffsets", "latest")
      .load()

    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

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
      } else None
    }

    val query = parsedDS.writeStream
      .foreachBatch { (batchDF: Dataset[StockData], batchId: Long) =>
        val rowCount = batchDF.count()
        println(s"🔥 Batch $batchId reçu avec $rowCount lignes")

        if (rowCount > 0) {
          try {
            val aggDF = batchDF
              .withColumn("prix_pondere", $"close" * $"volume")
              .groupBy($"ticker")
              .agg(
                count(lit(1)).as("nb_enregistrements"),
                avg($"volume").as("volume_moyen"),
                max($"high").as("plus_haut"),
                min($"low").as("plus_bas"),
                sum($"prix_pondere").as("somme_close_volume"),
                sum($"volume").as("somme_volume"),
                sum($"transactions").as("transactions_totales"),
                first($"open").as("ouv"),
                last($"close").as("ferm")
              )
              .withColumn("vwap", $"somme_close_volume" / $"somme_volume")
              .drop("somme_close_volume", "somme_volume")
              .withColumn("volatibilite", ($"plus_haut" - $"plus_bas") / $"ouv")
              .withColumn("volatibilite_pct", $"volatibilite" * 100)
              .withColumn("roi_simule", (($"ferm" - $"ouv") / $"ouv") * 100)
              .withColumn("drawdown", (($"ferm" - $"plus_haut") / $"plus_haut") * 100)
              .withColumn("typical_price", (($"plus_haut" + $"plus_bas" + $"ferm") / 3))
              .withColumn("batch_id", lit(batchId))
              .withColumn("date_calc", current_timestamp())

            // Insertion dans PostgreSQL
            aggDF.write
              .format("jdbc")
              .option("url", dbUrl)
              .option("dbtable", "public.stock_data_agg")
              .option("user", dbUser)
              .option("password", dbPassword)
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save()

            batchDF.write
              .format("jdbc")
              .option("url", dbUrl)
              .option("dbtable", "public.stock_data")
              .option("user", dbUser)
              .option("password", dbPassword)
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save()

            println(s"📊 Batch $batchId : données insérées ✅")

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
