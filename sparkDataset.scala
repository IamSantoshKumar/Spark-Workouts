package com.inceptez.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//Dataset - Strongly typed so there is type safety. 
//It has encoders 
//Dataset only there in java and scala, python and R does not have dataset

object Lab13 {

  def main(args:Array[String])=
  {
    
    val spark = SparkSession.builder().appName("encoders-ex").master("local[*]").getOrCreate()
    
    val df = spark.read.format("csv").option("inferSchema",true).
    load("file:/home/hduser/hive/data/custs").toDF("custid","custfname","custlname","custage","custprofession")
    
   val rdd = spark.sparkContext.textFile("file:/home/hduser/hive/data/custs") 
    
    //implicits contains encoders
    import spark.implicits._
    
    //Creating Dataset from Dataframe
    val ds1 = df.as[Customer]
    
    //Creating DataSet from collections
    
    val movies = Seq(Movie(1001,"King Kong",3.4,7095,1976),Movie(1002,"Titanic",4.4,4043,1995))
    val ds2 = spark.createDataset(movies)
    val ds3 = movies.toDS()
    
    //Creating DataSet from RDD
    val rdd1 = rdd.map(x => x.split(",")).map(x => Customer(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    val ds4 = rdd1.toDS()
    
    //There is no type-safety
    val df1 = df.filter(col("custprofession") === "Teacher" && col("custage") > 40)
    
        //There is type-safety
    val df2 = ds1.filter(x => x.custprofession == "Teacher" && x.custage > 40)
    
    
    
    //Convert Dataset into DataFrame
    val df3 = ds4.toDF()
    val df4 = df3.filter(col("custprofession") === "Teacher" && col("custage") > 40)
    
    //Convert Dataset into RDD
    val rdd2 = ds4.rdd
    val rdd3 = ds1.filter(x => x.custprofession == "Teacher" && x.custage > 40)
    
    
    
  }
}
case class Customer(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
case class Movie(movieID:Int,movieName:String,movieRating:Double,movieDuration:Int,movierelease:Int)
