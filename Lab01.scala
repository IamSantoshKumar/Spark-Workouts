package com.inceptez.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Lab1 {
  
  def main(args:Array[String])
  {
   val conf = new SparkConf().setAppName("local-lab01")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")
   val rdd = sc.textFile("file:/home/hduser/hive/data/txns",10)
   //rdd.foreach(println)
   rdd.collect().foreach(println)
  }
}