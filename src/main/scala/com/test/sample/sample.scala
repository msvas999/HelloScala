package org.sparktest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._



object sample {
  case class YahooStockPrice(SOM:String, count:String, DCUNo:String, MtrNo:String, MtrDt:String)
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sample")
	 conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    
    //val sqlc = new org.apache.spark.sql.SQLContext(sc)
    val hc = new HiveContext(sc)
        
    hc.setConf("hive.metastore.uris", "thrift://localhost:9083")
    hc.sql("use poc")
    import hc.implicits._
       
    val yahoo_stocks=sc.textFile("/root/Data/")
    val header =yahoo_stocks.first
// val data =yahoo_stocks.filter(_ != header)

  //import sqlc.implicits._
  
   
val stockprice=yahoo_stocks.map(_.split(",")).map(row =>YahooStockPrice(row(0), row(1).trim.toString, row(2).trim.toString, row(3).trim.toString, row(4).trim.toString))

val smdf = stockprice.toDF()

smdf.show(10)
smdf.write.mode(SaveMode.Overwrite).saveAsTable("Sample_test")
println("Job Completed")
  }
}