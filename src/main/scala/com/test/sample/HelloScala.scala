package com.test.sample

import org.apache.spark.{SparkConf, SparkContext}

object HelloScala {
  def main(args: Array[String]):Unit = {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val data = sc.textFile("D:\\Practice\\RawData\\test.txt")
    //word count
    val counts = data.flatMap(line => line.split(" ")).map(word => (word,1))
    //counts.collect.foreach(println) (a,b) => a+b
    val res = counts.reduceByKey(_+_)
    res.collect.foreach(println)
    //System.out.println("Total words: " + counts.count())
    //counts.saveAsTextFile("D://Practice/results/wordcount")
  }
}
