package com.test.sample

object TestScala {

  def main(args:Array[String]):Unit = {
    for (i <- 1 to 3)
      for(j <- 1 to 3)
      println(i,j)
  }

}
