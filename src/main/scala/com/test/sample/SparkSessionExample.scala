package com.test.sample

import org.apache.spark.sql.SparkSession

object SparkSessionExample {
  import org.apache.spark._
  import org.apache.spark.sql.hive._
  import org.apache.spark.sql._
  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("SparkSessionExample").config("spark.master","local").getOrCreate()
    val df = spark.read
      .option("header", "true")
      .csv("D://Practice//RawData//sample.txt")

    df.show()

  }

}
