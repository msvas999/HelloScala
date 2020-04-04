package com.test.sample

object my_function {
  def main(args:Array[String]):Unit = {
    def add(a:Double = 100,b:Double = 200) : Double = {

      var sum:Double = 0
      sum = a+b
      return sum
    }
    println("SUM :" +add())
  }

}
