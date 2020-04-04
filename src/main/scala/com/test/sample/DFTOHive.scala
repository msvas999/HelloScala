package com.test.sample
import com.test.sample.SparkHiveIntegration.YahooStockPrice
import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.sql._

object DFTOHive {
  case class YahooStockPrice(SOM:String, count:String, DCUNo:String, MtrNo:String, MtrDt:String)

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("DFtoHive").setMaster("local")
      val sc = new SparkContext(conf)
     /*val spark = SparkSession.builder
        .master("local")
        .appName("demo")
        .enableHiveSupport()
        .getOrCreate()
      spark.sql("create database poc")
      spark.sql("use poc")
      import spark.implicits._*/

      val data = sc.textFile("D://Practice//RawData//samplewoheader.txt")
      //val stockprice=data.map(_.split(",")).map(row =>YahooStockPrice(row(0), row(1).trim.toString, row(2).trim.toString, row(3).trim.toString, row(4).trim.toString))
     // val filterData = data.map(_.split(",")).map(y=>y(1) +","+y(3))
     // val schData=data.map(_.split(",")).map(row =>YahooStockPrice(row(1).trim.toString, row(3).trim.toString))
     // filterData.collect.foreach(println)
      //smdf.show(10)
      /*hc.sql("create table Sample_test (SOM string,Count string,DCUNo string,MtrNo string,MtrDt string)")
      smdf.write.mode(SaveMode.Overwrite).saveAsTable("Sample_test")
      smdf.write.mode(SaveMode.Overwrite).format("csv").save("D://Practice//results//sample")
      hc.sql("select * from Sample_test").show()*/
      data.collect.foreach(println)
      println("Job Completed")
    }

}
