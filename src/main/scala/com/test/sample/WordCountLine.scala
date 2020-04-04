package com.test.sample

import org.apache.spark.{SparkConf, SparkContext}

object WordCountLine {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("WordCounteachLine").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //val data = sc.textFile("D:\\Practice\\RawData\\Employee.txt")
    val data = sc.textFile("RawData/test.txt")
   // data.collect().foreach(println)
   val output = data.map(_.split(" ").map((_, 1)).groupBy(_._1)
     .map { case (group: String, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }.toList).flatMap(tuple => tuple)
    output.collect.foreach(println)
  }

}

