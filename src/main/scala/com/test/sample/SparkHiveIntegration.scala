package com.test.sample

import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.sql._

object SparkHiveIntegration {
  case class YahooStockPrice(SOM:String, count:String, DCUNo:String, MtrNo:String, MtrDt:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sample")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    //val sqlc = new org.apache.spark.sql.SQLContext(sc)
    val hc = new HiveContext(sc)
    /*val spark = SparkSession.builder
      .master("local")
      .appName("demo")
      .enableHiveSupport()
      .getOrCreate()*/

    //hc.setConf("hive.metastore.uris", "jdbc:hive2://localhost:10000")
    hc.sql("create database poc")
    hc.sql("use poc")
    import hc.implicits._

    val yahoo_stocks=sc.textFile("D://Practice//RawData//sample.txt")
    //val yahoo_stocks=sc.textFile("root/data//sample.txt")
    //yahoo_stocks.collect.foreach(println)
    val header =yahoo_stocks.first
    val data =yahoo_stocks.filter(_ != header)

    //import sqlc.implicits._

    val stockprice=data.map(_.split(",")).map(row =>YahooStockPrice(row(0), row(1).trim.toString, row(2).trim.toString, row(3).trim.toString, row(4).trim.toString))
    //val stockprice=data.map(_.split(","))
    //stockprice.collect.foreach(println)
    val smdf = stockprice.toDF()
    //smdf.show(10)
    hc.sql("create table Sample_test (SOM string,Count string,DCUNo string,MtrNo string,MtrDt string)")
    smdf.write.mode(SaveMode.Overwrite).saveAsTable("Sample_test")
    smdf.write.mode(SaveMode.Overwrite).format("csv").save("D://Practice//results//sample")
    hc.sql("select * from Sample_test").show()
    println("Job Completed")
  }
}
