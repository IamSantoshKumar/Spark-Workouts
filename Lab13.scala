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
object Lab13 {
  
  def main(args:Array[String]) =
  {
    val conf = new SparkConf().setAppName("Lab11-Excercise").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   
    val rdd = sc.textFile("file:/home/hduser/mrdata/courses.log")
    val rdd1 = rdd.flatMap(x => x.split(" "))
    val rdd2 = rdd1.map(x => (x,1))
    val rdd3 = rdd2.reduceByKey((a,b) => a + b,1)
    val rdd4 = rdd3.sortBy(x => x._2)
    rdd4.foreach(println)
    
    
    
    
    
    
  }
  
}