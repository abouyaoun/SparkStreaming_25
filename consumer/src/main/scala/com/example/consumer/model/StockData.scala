package com.example.consumer.model

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