package com.example.consumer.utils

import com.example.consumer.model.StockData
import scala.util.Try

object MessageParser {
  def parseLine(line: String): Option[StockData] = {
    val parts = line.split(",")

    if (parts.length == 8) {
      Try {
        Some(StockData(
          ticker       = parts(0),
          window_start = parts(1).toLong,
          open         = parts(2).toDouble,
          high         = parts(3).toDouble,
          low          = parts(4).toDouble,
          close        = parts(5).toDouble,
          volume       = parts(6).toLong,
          transactions = parts(7).toLong
        ))
      }.getOrElse(None)
    } else {
      None
    }
  }
}