package sparkstreaming
import org.apache.spark.SparkConf
import  org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.{KafkaProducer,
ProducerConfig, ProducerRecord}


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get

import java.util.Properties
import java.util.HashMap
import java.sql.{Connection, DriverManager, ResultSet}
/*
 Read from Kafka -> Lookup with hbase -> Write into another kafka topic
 */

object lab92 {
  
  def main(args:Array[String])
  {
        val sparkConf = new SparkConf().setAppName("kafkahbase").setMaster("local[*]")
        val sparkcontext = new SparkContext(sparkConf)
        sparkcontext.setLogLevel("ERROR")
        
        val ssc = new StreamingContext(sparkcontext, Seconds(10))
        ssc.checkpoint("checkpointdir")
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "kafkatest1",
          "auto.offset.reset" -> "earliest"
          )

        val topics = Array("test")
        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
        val kafkastream = stream.map(record => (record.key, record.value))
        val inputStream = kafkastream.map(rec => rec._2);
        
        inputStream.foreachRDD (rdd1 => 
          {
            if(!rdd1.isEmpty)
            {
              rdd1.foreachPartition(row => 
              {
                 val conf = HBaseConfiguration.create()
                 conf.set("hbase.zookeeper.quorum", "localhost:2181")
                 conf.set("hbase.master", "localhost:60000");
                 conf.set("hbase.rootdir", "file:///tmp/hbase")
                 val table = new HTable(conf, "tbltxns")
                 val parsedrow = row.map(x => 
                   {
                     val theget = new Get(Bytes.toBytes(x))
                     val result=table.get(theget)
                     TransRow.parseTransRow(result)
                   }                 
                 )
                 //rdd2.foreach(println)
                 //write into kafka topic
                   val props = new HashMap[String, Object]()
                          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
                          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            "org.apache.kafka.common.serialization.StringSerializer")
                          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                            "org.apache.kafka.common.serialization.StringSerializer")
                          val producer = new KafkaProducer[String,String](props)
                
                for (trans <- parsedrow) {
                              val str = "{\"txnid\":" + trans.transid +
                        ",\"txndate\":\"" + trans.transdate + "\",\"custid\":\"" + trans.custid +
                        "\",\"price\":\"" + trans.salesamt + "\",\"product\":\"" + trans.prodname +
                        "\",\"category\":\"" + trans.category + "\",\"city\":\"" + trans.city +
                        "\",\"state\":\"" + trans.state + "\",\"paymenttype\":\"" + trans.payment +
                        "\"}";
                            val message=new ProducerRecord[String,String]("test1",null,str);
                            producer.send(message)
                 }                 
               })
              }
            }  
        );
        //println("test")
        ssc.start()
        ssc.awaitTermination()
  }


case class transRow(transid: String, transdate: String, custid: String, salesamt: Float, category: String, prodname: String, state: String, city: String,payment: String)

  object TransRow extends Serializable {
    def parseTransRow(result: Result): transRow = {
      val rowkey = Bytes.toString(result.getRow())
      // remove time from rowKey, stats row key is for day
      val p0 = rowkey.split(" ")(0)
      val p1 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("txndate")))
      val p2 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("custid")))
      val p3 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("amount")))
      val p4 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("category")))
      val p5 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("prodname")))
      val p6 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("state")))
      val p7 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("city")))
      val p8 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("payment")))
      transRow(p0, p1, p2, p3.toFloat, p4, p5, p6,p7,p8)
    }
  }
}