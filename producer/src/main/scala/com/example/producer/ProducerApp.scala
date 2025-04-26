package com.example.sparkcoreproducer

import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.mutable.ArrayBuffer

object ProducerApp {
  def main(args: Array[String]): Unit = {
    println("🚀 Lancement du Producer Scala vers Kafka")

    // Lecture du fichier CSV
    //val lines = scala.io.Source.fromFile("/data/dataset_cac40/CAC40_stocks_2010_2021.csv").getLines().toList
    val lines = scala.io.Source.fromFile("/data/dataset_stock/2025-04-11.csv").getLines().toList
    val header = lines.head
    val data = lines.tail

    // Kafka Producer config
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Diviser en batches de 100
    val batches = data.grouped(100).toList

    for ((batch, i) <- batches.zipWithIndex) {
      println(s"📦 Envoi du batch $i (${batch.size} lignes)")
      batch.foreach { line =>
        val record = new ProducerRecord[String, String]("my_topic", null, line)
        producer.send(record)
        println(s"📤 ligne envoyée : $line")
      }
      Thread.sleep(2000)
    }

    producer.close()
    println("✅ Fin de l'envoi Scala -> Kafka")
  }
}