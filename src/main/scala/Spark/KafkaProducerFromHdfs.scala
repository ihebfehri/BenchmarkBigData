package Spark

import java.util.{Date, Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


object KafkaProducerFromHdfs {

  //hive location: hdfs://hadoop-c1.talan:8020/apps/hive/warehouse/table_test

  def main(args: Array[String]): Unit = {

    //    val config = {
    //      val properties = new Properties()
    //      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
    //      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-c1.talan:2181") // was 'localhost' before
    //      properties
    //    }
    //
    //    val builder = new KStreamBuilder()
    //    val sourceStream = builder.stream("SourceTopic")
    //    sourceStream.to("SinkTopic")
    //
    //    val streams = new KafkaStreams(builder, config)
    //    streams.start()
    //  }

    val scc = SparkSession
      .builder()
      .master("local")
      .appName("generateDataRATP")
      .config("hive.metastore.uris", "thrift://hadoop-c2.talan:9083")
      .config("spark.sql.warehouse.dir", "/Users/ihebfehri/Desktop/GeneratedDataBenchmark/src/main/scala/Spark/utils/hive-site.xml")
      //    .config("hive.metastore.uri", "thrift://sandbox-hdp.hortonworks.com:9083")
      //    .config("hive.metastore.warehouse.dir", "172.168.0.2" + "user/hive/warehouse")
      //      .config("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    //
    //     val lines = .createStream(scc.sparkContext,"167.114.227.216:2181", "spark-streaming-consumer-group", Map("customer" -> 5))
    //     lines.print()

    val events = args(0).toInt
    val topic = args(1)
    val brokers = args(2)
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()

    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](topic, ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()

  }

}