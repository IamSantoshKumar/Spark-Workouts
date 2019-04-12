package com.inceptez.spark.core
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
/*
  PairRDD - pair RDD arranges the data of a row into two parts. 
  The first part is the Key and the second part is the Value
  
  There are set of transformation that are applied only on top of pair RDD's.
  Few are:
  1. groupBykey()
  2. reduceByKey()
  3. sortByKey()
  4. join()
  
 */
object Lab11 {
  
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab11-Excercise").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   
    val rdd = sc.textFile("file:/home/hduser/hive/data/txns")
    val rdd1 = rdd.map(x => x.split(","))
    val rdd2 = rdd1.map(x => (x(7),1))
    val rdd3 = rdd2.reduceByKey((a,b) =>(a + b))
    rdd3.foreach(println)
  }
  
  def groupbyfn(rdd:RDD[String])=
  {
       val rdd1 = rdd.map(x => x.split(","))
       val rdd2 = rdd1.map(x => (x(7),1))
       val rdd3 = rdd2.groupByKey()
       val rdd4 = rdd3.map(x => (x._1,x._2.sum))
       rdd4.foreach(println)
       
  }
  def sortbyfn(rdd:RDD[String])=
  {
       val rdd1 = rdd.map(x => x.split(","))
       val rdd2 = rdd1.map(x => (x(7),1))
       val rdd3 = rdd2.reduceByKey((a,b) =>(a + b))
       val rdd4 = rdd3.sortByKey(false)
       rdd3.foreach(println)
       
  }
  /*
   This transformation is used to join the information of two datasets. 
   By joining the dataset of type (K,V) and dataset (K,W), the result of the joined dataset is (K,(V,W)).
   * */
  def joinfn(sc:SparkContext)=
  {
     val rdd1 = sc.parallelize(Seq(("math",55),("math",56),("english", 57),("english", 58),("science", 59),("science", 54)))
     val rdd2 = sc.parallelize(Seq(("math",60),("math",65),("science", 61),("science", 62),("history", 63), ("history", 64)))
     val joined = rdd1.join(rdd2)
     joined.collect()
     val leftJoined = rdd1.leftOuterJoin(rdd2)
     leftJoined.collect()
     val rightJoined = rdd1.rightOuterJoin(rdd2)
     rightJoined.collect()
  }
}