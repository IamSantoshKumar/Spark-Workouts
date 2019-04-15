package com.spark.hackthon

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
//import org.apache.spark.sql
import org.apache.spark.sql.SparkSession


object exp1 {
  def main(args:Array[String]){
  
 val spark=SparkSession.builder().master("local[*]").appName("Hackthon1").getOrCreate()
 //val df1=spark.read.format("csv")
 //       .option("delimiter",",")
 //       .load("file://home/hduser/sfd.csv")
 //       .toDF("incidentnum","category","description","dayofweek","date","time","pddistrit","district","resolution","address","X","Y","pdid")
 // val conf=new SparkConf().setMaster("local[*]").setAppName("Hack1")
 // val sc=new SparkContext(conf)
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val rdd=spark.sparkContext.textFile("file:/home/hduser/sfpd.csv")
  val rdd1=rdd.map(x=>x.split(","))
  val rdd2=rdd1.map(x=>Incidents(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9).toFloat,x(10).toFloat,x(11)))
  val rdd2df=spark.createDataFrame(rdd2)
  //rdd2df.show(10,false)
  val df2DS=rdd2df.as[Incidents]
  //df2DS.show(10,false)
  val incbydist=df2DS.groupBy("pddistrict").agg(count("incidentnum").alias("incidentnumcnt")).sort(desc("incidentnumcnt")).limit(5)
  //incbydist.show(10,false)
  df2DS.createOrReplaceTempView("sfpd")
  val dfincbydistSQL4=spark.sql("select pddistrict,count(incidentnum) as incidentnumcnt from sfpd group by pddistrict order by incidentnumcnt desc").limit(5)
  
  
  val incbyTop10ResDist=df2DS.groupBy("resolution").agg(count("incidentnum").alias("incidentnumcount")).sort(desc("incidentnumcount")).limit(10)
  val dfincbyTop10ResDistSQL=spark.sql("select resolution,count(incidentnum) as incidentnumcnt from sfpd group by resolution order by incidentnumcnt desc").limit(10)
  
  //dfincbyTop10ResDistSQL.show(10,false)
 
  val incbyTop3Cat=df2DS.groupBy("category").agg(count("incidentnum").alias("incidentnumcount")).sort(desc("incidentnumcount")).limit(3)
  val incbyTop3CatSQL=spark.sql("select category,count(incidentnum) as incidentnumcnt from sfpd group by category order by incidentnumcnt desc").limit(3)
   
  
  val incbyTop10ResDistWarn=df2DS.where(col("category")==="WARRANTS")
  val dfincbyTop10ResDistSQLWarn=spark.sql("select * from sfpd where category='WARRANTS'")
  
  incbyTop10ResDist.write.json("hdfs://localhost:54310/user/hduser/hack1sfpd_json")
  dfincbyTop10ResDistSQLWarn.write.parquet("hdfs://localhost:54310/user/hduser/hack1sfpd_parquet")
  
  spark.udf.register("getyear",(inputdt:String)=>{inputdt.substring(inputdt.lastIndexOf('/')+1)})
  val incYearSQL=spark.sql("select getyear(Date),count(incidentnum) as countbyyear from sfpd group by getyear(Date) order by countbyyear desc")
  //incYearSQL.show(10,false)
  val inc2014=spark.sql("select getyear(Date),incidentnum,category,resolution from sfpd where getyear(Date)='14'")
  val inc2015=spark.sql("select getyear(Date),incidentnum,address,resolution from sfpd where getyear(Date)='15' and resolution='VANDALISM'")
  
  spark.udf.register("getmonth",(inputdt:String)=>{inputdt.substring(0,inputdt.indexOf('/'))})
  val inc20141=spark.sql("select getmonth(Date),count(incidentnum) as incidentnumcnt from sfpd where getyear(Date)='14' group by getmonth(Date) order by incidentnumcnt desc")
  inc20141.show(10,false)
  }
}
case class Incidents(incidentnum:String,category:String,description:String,dayofweek:String,Date:String,time:String,pddistrict:String,resolution:String,address:String,x: Float,y:Float,pdid:String)
