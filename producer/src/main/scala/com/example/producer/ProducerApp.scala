package com.example.sparkcoreproducer

import org.apache.spark.sql.{SparkSession}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object ProducerApp {
  def main(args: Array[String]): Unit = {
    println("Lancement du Producer Scala avec Spark vers Kafka")

    val spark = SparkSession.builder()
      .appName("Spark Kafka Producer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Lire le CSV avec Spark
    val df = spark.read
      .option("header", "true")
      .csv("/data/dataset_stock/2025-04-11.csv")

    println(s"Nombre de lignes lues : ${df.count()}")

    // Envoyer chaque partition séparément
    df.map(row => row.mkString(","))
      .rdd
      .foreachPartition { partitionIterator =>

        val props = new Properties()
        props.put("bootstrap.servers", "kafka:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)


        //envoie ligne par ligne mauvais, utiliser un timestemp pour comparer,
        partitionIterator.grouped(100).foreach { batch =>
          println(s"Envoi d'un batch de ${batch.size} lignes")
          batch.foreach { line =>
            val record = new ProducerRecord[String, String]("my_topic", null, line)
            producer.send(record)
          }
          Thread.sleep(5000)
        }

        producer.close()
      }

    spark.stop()

    println("Fin de l'envoi Spark -> Kafka")
  }
}
