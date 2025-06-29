package com.example.sparkcoreproducer

import org.apache.spark.sql.{SparkSession, Encoders}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProducerApp {

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

  def main(args: Array[String]): Unit = {
    println("🚀 Lancement du Producer Scala avec Spark vers Kafka")

    val spark = SparkSession.builder()
      .appName("Spark Kafka Producer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .schema(Encoders.product[StockData].schema) // impose un schéma clair
      .csv("/data/dataset_stock/2025-04-11.csv")
      .orderBy($"window_start".cast("long"))

    println(s"✅ ${df.count()} lignes lues")

    // Transforme en JSON
    val jsonRDD = df.toJSON.rdd

    jsonRDD.foreachPartition { partitionIterator =>
      val props = new Properties()
      props.put("bootstrap.servers", "kafka:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      partitionIterator.grouped(100).foreach { batch =>
        println(s"🧾 Envoi d'un batch de ${batch.size} lignes")
        batch.foreach { json =>
          val record = new ProducerRecord[String, String]("my_topic", null, json)
          producer.send(record)
        }
        Thread.sleep(5000)
      }

      producer.close()
    }

    spark.stop()
    println("✅ Fin de l'envoi Spark → Kafka")
  }
}