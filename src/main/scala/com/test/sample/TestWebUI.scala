package com.test.sample

import org.apache.spark.{SparkConf, SparkContext}

object TestWebUI {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("TestWebUI").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("RawData/shakespeare.txt")
    data.collect().foreach(println)

  }

}
