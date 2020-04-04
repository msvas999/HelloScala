package com.test.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SchemaStruct {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("SchemaStruct").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //val hc = new HiveContext(sc)
    val spark = SparkSession.builder
      .master("local")
      .appName("demo")
     // .enableHiveSupport()
      .getOrCreate()

    val employee = sc.textFile("D://Practice//RawData//Employee.txt")
    employee.collect().foreach(println)
    val schemaString = "id name age"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType)))
    val rowRDD = employee.map(_.split(",")).map(e ⇒ Row(e(0), e(1), e(2)))
    val employeeDF = spark.createDataFrame(rowRDD, schema)
    //hc.sql("create table empData(EmpNo int,Name string,age int)")
    //employeeDF.write.mode(SaveMode.Overwrite).saveAsTable("empData")
    employeeDF.createOrReplaceGlobalTempView("emptest")
    spark.sql("SELECT * FROM emptest").show()
  }
}
