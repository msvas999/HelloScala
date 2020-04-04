package com.test.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object LoadTabletoDF {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("WordCounteachLine").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()
    val tableDf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "userwithoutnum", "keyspace" -> "userstream"))
      .load()
    tableDf.show();
  }
}
