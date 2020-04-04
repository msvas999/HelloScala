package com.test.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions


object DatasetAddColumn {
  def main(args: Array[String]): Unit = { // configure spark
    val conf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.appName("Spark Example - Add a new Column to Dataset").master("local[2]").getOrCreate
    val jsonPath = "D:\\Practice\\RawData\\Employee.txt"
    val ds = spark.read.csv(jsonPath)
    // dataset before adding enw column
    ds.show()
    // add column to ds
    val newDs = ds.withColumn("Salary", functions.lit(15000))
    //add_column(ds, "salary",.after=1)
    // print dataset after adding new column
    newDs.show()
    spark.stop()
  }
}