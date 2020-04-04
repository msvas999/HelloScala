package com.test.sample

object Demo {
  def main(args: Array[String]) {
    println(matchTest(2))
  }

  def matchTest(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }
}