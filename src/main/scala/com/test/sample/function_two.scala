package com.test.sample

object function_two {
  def main(args:Array[String]):Unit = {
    println("Main function: "+exec(time100()))
    def time(): Long = {
      println("In the time function")
      return System.nanoTime()
    }
    def time100():Long = {
      println("In the time100 function")
      return System.nanoTime()
    }
    def exec(t: => Long):Long = {
      println("Enter exec function")
      println("Time: "+t)
      println("calling again")
      return t
    }
  }

}
