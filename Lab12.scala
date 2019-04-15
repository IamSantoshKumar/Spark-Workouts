package com.inceptez.spark.core
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel._
/*
  Cache and Persistence:
  Spark RDD cache is an optimization technique in which saves the result of RDD evaluation.
  Using this we save the intermediate result so that we can use it further if required. 
  It reduces the computation overhead.
  Advantages:-
    Time efficient
    Cost efficient
    Lessen the execution time.
  We can make persisted RDD through cache() and persist() methods.
 * */
object Lab12 {
  
  def main(args:Array[String]) =
  {
    val conf = new SparkConf().setAppName("Lab11-Excercise").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   
    val rdd = sc.textFile("file:/home/hduser/hive/data/txns")
    val rdd1 = rdd.map(x => x.split(","))
    rdd1.cache() 
    val rdd2 = rdd1.filter(x => x(7) == "California")
    rdd2.foreach(println)
    
    val rdd3 = rdd1.filter(x => x(7) == "Texas")
    rdd3.foreach(println)
    
  }
  
}