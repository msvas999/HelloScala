package com.test.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.kafka.clients.consumer._
import org.apache.spark.sql.types.StructType

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.StructType
import java.lang.IllegalArgumentException
import org.apache.spark._

import org.apache.spark.SparkDriverExecutionException
import java.lang._
import com.datastax.spark.connector.CassandraRow

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.explode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.{ ProducerRecord, KafkaProducer }
import org.apache.kafka.common.serialization.{ StringSerializer, StringDeserializer }
import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException
import java.io._
import org.apache.spark._
import org.apache.spark.sql._
import scala.util.Try
import org.apache.spark.sql.types.DecimalType._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.StructType
import java.lang.IllegalArgumentException
import org.apache.spark._

import org.apache.spark.SparkDriverExecutionException
import java.lang._
import com.datastax.spark.connector.CassandraRow

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import java.util.Properties

object KafkaSparkStreamCassApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    import spark.implicits._

    val ssc = new StreamingContext(conf, Seconds(2))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](properties)

    val TOPIC = "usernames"
    val topicwo = "usernameswo"
    val topics = Array("sampletest2")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val streamdata = stream.map(record => (record.value))
    //streamdata.print()
    streamdata.foreachRDD(rdd =>
      if (!rdd.isEmpty()) {
        val data = spark.read.json(rdd)
        val withid = data.withColumn("results", explode($"results")).select("results.user.username")
        val rddstring = withid.rdd.collect().mkString("")
        val record = new ProducerRecord(TOPIC, "key", rddstring)
        producer.send(record)
        println("written to kafka")
        withid.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "userstream", "table" -> "userwithnum")).mode("append").save()
        val withoutid = data.withColumn("results", explode($"results")).select("results.user.username")
          .withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
        withoutid.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "userstream", "table" -> "userwithoutnum")).mode("append").save()
        println("data written")
        val rddstring1 = withoutid.rdd.collect().mkString("")
        val record1 = new ProducerRecord(topicwo, "key", rddstring1)
        producer.send(record1)
      })
    ssc.start()
    ssc.awaitTermination()

  }
}
