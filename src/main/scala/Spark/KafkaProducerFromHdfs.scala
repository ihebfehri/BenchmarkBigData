package Spark

import java.util.Properties
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams._


object KafkaProducerFromHdfs {

  //hive location: hdfs://hadoop-c1.talan:8020/apps/hive/warehouse/table_test

   def main(args: Array[String]): Unit = {

    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-c1.talan:2181") // was 'localhost' before
      properties
    }

    val builder = new KStreamBuilder()
    val sourceStream = builder.stream("SourceTopic")
    sourceStream.to("SinkTopic")

    val streams = new KafkaStreams(builder, config)
    streams.start()
  }





}

//
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark._
////import java.util.stream.StreamSpliterators.ArrayBuffer
//import scala.collection.mutable.ArrayBuffer
//
//
//object teststreaming {
//
//  def main(args: Array[String]) {
//
//    System.setProperty("hadoop.home.dir", "c://winutil//")
//    val conf = new SparkConf().setAppName("Application").setMaster("local[2]")
//    //val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(conf,Seconds(30))
//    val input=ssc.textFileStream("file:///C://Users//HA848869//Desktop//sparkdata//")
//    val lines=input.flatMap(_.split(" "))
//    val words=lines.map(word=>(word,1))
//    val counts=words.reduceByKey(_+_)
//    counts.print()
//    val arr = new ArrayBuffer[String]();
//    ssc.start()
//    ssc.awaitTermination()
//
//
//  }
//
//}