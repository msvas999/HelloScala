package com.test.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TextFileRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sample")
    //conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val data = spark.read.csv("D:\\Practice\\RawData\\Employee.txt")
    //data.collect().foreach(println)
    data.show()
  }
  }
