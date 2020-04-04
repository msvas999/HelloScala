package com.test.sample

object test {
    def main(args:Array[String]):Unit = {
      println(sumInt(2,7))
      def id(x:Int):Int = x
        def sumInt(a:Int,b:Int):Int={
          println("Inside sumInt method")
          if(a > b)
            0
          else {
           id(a) + sumInt(a + 1, b)
          }
        }
    }

}
