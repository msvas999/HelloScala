package com.test.sample

import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.sql._

object HiveToHiveApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HivetohiveApp").config("spark.master","local").enableHiveSupport().getOrCreate()
    spark.sql("CREATE TABLE  IF NOT EXISTS Employee (EmpNo int,Name string) ") //Handling from Hive
    spark.sql("insert into Employee values (101,'Srinivas')") //Handling from Hive
    spark.sql("insert into Employee values (102,'Naresh')") //Handling from Hive
    val empData = spark.sql("select * from Employee")
    spark.sql("create table EmpNames(Name string)") //Handling from Hive
    empData.select("name").write.mode(SaveMode.Overwrite).saveAsTable("EmpNames")
    spark.sql("select * from EmpNames").show();
  }
}

