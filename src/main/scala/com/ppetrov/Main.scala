package com.ppetrov

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkTestTask").getOrCreate()
    StocksService.init(spark)
  }
}
