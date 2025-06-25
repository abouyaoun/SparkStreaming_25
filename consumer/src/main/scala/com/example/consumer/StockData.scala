package com.example.consumer

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