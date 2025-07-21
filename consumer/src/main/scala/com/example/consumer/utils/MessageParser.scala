package com.example.consumer.utils

import com.example.consumer.model.StockData
import scala.util.Try

object MessageParser {
  def parseLine(line: String): Option[StockData] = {
    val parts = line.split(",")
    if (parts.length == 8) {
      Try(Some(StockData(
        parts(0),
        parts(1).toLong,
        parts(2).toDouble,
        parts(3).toDouble,
        parts(4).toDouble,
        parts(5).toDouble,
        parts(6).toLong,
        parts(7).toLong
      ))).getOrElse(None)
    } else {
      None
    }
  }
}