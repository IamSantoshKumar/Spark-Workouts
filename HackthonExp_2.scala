package com.spark.hackthon
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
object exp2 {
def main(args:Array[String]){
  val spark=SparkSession.builder().master("local[*]").appName("Hackthon2").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val rdd1=spark.sparkContext.textFile("file:/home/hduser/insuranceinfo1.csv") 
  val rdd4=spark.sparkContext.textFile("file:/home/hduser/insuranceinfo2.csv") 
  //val rdd2=rdd1.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
  val header1=rdd1.first()
  val header2=rdd1.first()
  val rdd3=rdd1.filter(x=> !(x.contains(header1)))
  val rdd5=rdd1.filter(x=> !(x.contains(header2)))
  println(rdd3.count())
  rdd3.take(10).foreach(println)
  println(rdd5.count())
  rdd5.take(10).foreach(println)
  
  
  val insuredatamerged=rdd3.union(rdd5)
  insuredatamerged.cache()
  insuredatamerged.foreach(println)
  println(insuredatamerged.count())
  val insuredatamergedpart=rdd3.distinct()
  println(insuredatamergedpart.count())
  val insuredatamergedpart1=insuredatamergedpart.repartition(5)
  val ins1=insuredatamergedpart1.map(x=>x.split(",")).filter(x=>x.length==10).map(x=>insure(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
  println(insuredatamergedpart1.partitions.length)
  
  val customer_states=spark.sparkContext.textFile("file:/home/hduser/custs_states.csv") 
  //customer_states.foreach(println)
  val custfilter=customer_states.map(x=>x.split(",")).filter(x=>x.length==5).map(x=>(x(0),x(1),x(2),x(3),x(4)))
  val statesfilter=customer_states.map(x=>x.split(",")).filter(x=>x.length==2).map(x=>(x(0),x(1)))
  //statesfilter.foreach(println)
  val insuredatamergedpart2=insuredatamergedpart.coalesce(1)
  println(insuredatamergedpart2.partitions.length)
  spark.udf.register("fun",fun)
  val dns=spark.createDataFrame(ins1)
  dns.createOrReplaceTempView("insuretb")
  val dffinal=spark.sql("select id1,id2,CONCAT(UPPER(stcde),'-',UPPER(nwnme)) as t,fun(url) as url from insuretb")
  dffinal.write.json("hdfs://localhost:54310/user/hduser/hack2sfpd1_json")
  dffinal.write.parquet("hdfs://localhost:54310/user/hduser/hack2sfpd2_parquet")
  dffinal.show()
}
  def fun= (stringdata:String)=>
    {
      stringdata.replace('a','z').replace('b','y').replace('c','x').replace('d','w').replace('e','v').replace('f','u').replace('g','t').replace('h','s').replace('i','r').replace('j','q').replace('k','p').replace('l','o').replace('m','n').replace('n','m')
      .replace('0','l').replace('p','k').replace('q','j').replace('r','i').replace('s','h').replace('t','g').
      replace('u','f').replace('v','e').replace('w','d').replace('x','c').replace('y','b').replace('z','a').
      replace('1','*').replace('2','@').replace('3','!').replace('4','/').replace('5','>').replace('6','<').
      replace('7',';').replace('8','&').replace('9','*').replace('0','=')
      
    }

}

case class insure(id1:String,id2:String,yr:String,stcde:String,srcnme:String,nwnme:String,url:String,rownum:String,market:String,dental:String)
