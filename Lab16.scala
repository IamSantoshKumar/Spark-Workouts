package com.inceptez.spark.core
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel._
/*
 Shared Variables: a variable needs to be shared across tasks, or between tasks and the driver program
 
 1. Broadcast -   A variable that is read-only copy is sent to the worker node to reference it in 
 the tasks.
 can be used to cache a value in memory on all nodes.
 
 2. Accumulator - These are write-only variables to store the result after aggregation (e.g. sum)
 * */
object Lab16 {
  
  def main(args:Array[String]) =
  {
    val conf = new SparkConf().setAppName("Lab16-Excercise").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    val accum = sc.longAccumulator("Accumulator Example")
    //broadcastVar.value 
    //accum.add(10)
    //accum.value
         
    
    
    
    
    
    
  }
  
}

/*
consider using broadcast variables under the following conditions:

		You have read-only reference data that does not change throughout the life of your Spark application.
    The data is used across multiple stages of application execution and would benefit from being locally cached on the worker nodes.
    The data is small enough to fit in memory on your worker nodes
 

*/
