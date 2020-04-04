package com.test.sample

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._

object HBaseConnect extends App {

  def printRow(result : Result) = {
    val cells = result.rawCells();
    print( Bytes.toString(result.getRow) + " : " )
    for(cell <- cells){
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print("(%s,%s) ".format(col_name, col_value))
    }
    println()
  }

  val conf : Configuration = HBaseConfiguration.create()

  val ZOOKEEPER_QUORUM = "WRITE THE ZOOKEEPER CLUSTER THAT HBASE SHOULD USE"
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("hbase.zookeeper.property.clientport","2181")

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf( Bytes.toBytes("emostafa:test_table") ) )

  // Put example
  var put = new Put(Bytes.toBytes("row1"))
  put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
  put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name2"), Bytes.toBytes("test_value2"))
  table.put(put)

  // Get example
  println("Get Example:")
  var get = new Get(Bytes.toBytes("row1"))
  var result = table.get(get)
  printRow(result)

  //Scan example
  println("\nScan Example:")
  var scan = table.getScanner(new Scan())
  scan.asScala.foreach(result => {
    printRow(result)
  })

  table.close()
  connection.close()
}
