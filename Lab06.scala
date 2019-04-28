package com.inceptez.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._
//for using col
import org.apache.spark.sql.functions.col
//for aggregation functions
import org.apache.spark.sql.functions._
/*
dataframe - dsl operations

 */

object Lab06 {
  
  def main(args:Array[String])
  {
    val conf = new SparkConf().setAppName("Lab06-SQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sql = new SQLContext(sc)
    
    //declare to  use $
    import sql.implicits._
    
    val df = sql.read.format("csv").
    load("file:/home/hduser/hive/data/custs").toDF("custid","custfname","custlname","custage","custprofession")
    
    val df1 = df.filter(col("custprofession") === "Teacher" && $"custage" > 40)
    //col("custage") > 40
    //col("custage") != 10
    //col("custage") between (20,30)
    //col("custage") isin (20,30)
    
    //Also we can use ' and $ to represent the column.. its sytanctic sugar for column class
    val df11 = df.filter('custprofession === "Teacher" && 'custage > 40)
    
    
    val df2 = df.where("custprofession == 'Teacher' and custage > 40")
    
    val df3 = df.filter($"custprofession" === "Teacher").where("custage > 40")
                .select("custid","custfname","custlname").sort(asc("custfname"),asc("custlname"))
    
    
    val df4 = df.groupBy("custprofession").agg(max("custage").alias("maxage"),min("custage").alias("minage"))
    
    //or
    
    val df5 = df.groupBy("custprofession").agg("custage" -> "max","custage" -> "min")
    
    df.selectExpr("min(custage) as minage","max(custage)").show()
    
    df.cache()
    df.unpersist()
    
    val df6 = df.na.fill(0)
    
    val df7 = df.na.drop()
    
    val count = df.select("custprofession").distinct.count
    
    println(count)
    df.printSchema()
    df.show(100,false)
  }
  
//Load txns data using case class and read method  
}

//val df = sql.read.format("csv").load("file:/home/hduser/hive/data/custs").toDF("custid","custfname","custlname","cusstage","custprofession")