package com.example.consumer.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Dataset}
import com.example.consumer.model.StockData

object BatchProcessor {
  def processBatch(batchDF: Dataset[StockData], batchId: Long)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

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
            last($"close").as("ferm"),
            max($"window_start").as("last_date")
          )
          .withColumn("vwap", $"somme_close_volume" / $"somme_volume")
          .drop("somme_close_volume", "somme_volume")
          .withColumn("volatibilite", ($"plus_haut" - $"plus_bas") / $"ouv")
          .withColumn("volatibilite_pct", $"volatibilite" * 100)
          .withColumn("roi_simule", (($"ferm" - $"ouv") / $"ouv") * 100)
          .withColumn("drawdown", (($"ferm" - $"plus_haut") / $"plus_haut") * 100)
          .withColumn("typical_price", (($"plus_haut" + $"plus_bas" + $"ferm") / 3))
          .withColumn("batch_id", lit(batchId))
          .withColumn("date_calc", to_timestamp(from_unixtime($"last_date" / 1e9)))

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
}