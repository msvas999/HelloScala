package com.test.sample

import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.sql._

object SparkHiveApp {

  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setMaster("local").setAppName("sample")
    //conf.set("spark.driver.allowMultipleContexts", "true")
    //val sc = new SparkContext(conf)
    //val hc = new HiveContext(sc)
    val spark = SparkSession.builder().appName("HivetohiveApp").config("spark.master","local").enableHiveSupport().getOrCreate()
    //hc.sql("show databases").show()
    //hc.sql("use poc")
    spark.sql("CREATE TABLE  IF NOT EXISTS Employee (EmpNo int,Name string) ")
   // hc.sql("show tables").show()
    spark.sql("insert into Employee values (101,'Srinivas')")
    spark.sql("insert into Employee values (102,'Naresh')")
    val empData = spark.sql("select * from Employee")
    spark.sql("create table EmpNames(Name string)")
    empData.select("name").write.mode(SaveMode.Overwrite).format("csv").saveAsTable("EmpNames")
    spark.sql("select * from EmpNames").show();

  }
}
