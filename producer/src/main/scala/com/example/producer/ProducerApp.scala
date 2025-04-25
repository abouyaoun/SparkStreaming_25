package com.example.sparkcoreproducer

import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.mutable.ArrayBuffer

object ProducerApp {
  def main(args: Array[String]): Unit = {
    println("🚀 Lancement du Producer Spark Core vers Kafka")

    // Configuration Spark Core
    val conf = new SparkConf().setAppName("SparkCSVKafkaProducer").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Lecture du fichier CSV avec Spark
    val path = "dataset_cac40/CAC40_stocks_2010_2021.csv"
    val rdd = sc.textFile(path)
    val header = rdd.first()
    val data = rdd.filter(_ != header)

    // Kafka Producer config
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092") // ou localhost:9092
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Diviser en batches de 100
    val batches = data.collect().grouped(100).toList

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
    sc.stop()
    println("✅ Fin de l'envoi Spark -> Kafka")
  }
}