package Spark

import org.apache.spark.sql.SparkSession

object SparkStreamingReadHdfs {
  import org.apache.spark.SparkConf
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.StreamingContext._
  import org.apache.spark._
  //import java.util.stream.StreamSpliterators.ArrayBuffer
  import scala.collection.mutable.ArrayBuffer

  def main(args: Array[String]) {
        if (args.length < 1) {
          System.err.println("Usage: please add 'String' at the end of a command to launch.")
          System.exit(1)
        }

        val conf = new SparkConf().setAppName("SparkDetectNewHdfsFiles").setMaster("yarn")

        val ssc = new StreamingContext(conf, Seconds(30))
        print("welcome to the "+ args(0) + ", my friend!")

        val input=ssc.textFileStream("hdfs://hadoop-c1.talan:8020/apps/hive/warehouse/t")
        input.print()

        val lines=input.flatMap(_.split(" "))
        val words=lines.map(word=>(word,1))
        val counts=words.reduceByKey(_+_)
        counts.print()
        val arr = new ArrayBuffer[String]();

        ssc.start()
        ssc.awaitTermination()

      }
//
  //  def main(args: Array[String]) {
  //    if (args.length < 1) {
  //      System.err.println("Usage: SparkStreamingReadHdfs <directory>")
  //      System.exit(1)
  //    }
  //  val sc = new SparkContext(conf)
  //  val sparkSession = SparkSession
  //    .builder()
  //    .master("local")
  //    .appName("HdfsWordCount")
  //    .config("hive.metastore.uris", "thrift://hadoop-c2.talan:9083")
  //    .config("spark.sql.warehouse.dir", "/Users/ihebfehri/Desktop/GeneratedDataBenchmark/src/main/scala/Spark/utils/hive-site.xml")
  //    //    .config("hive.metastore.uri", "thrift://sandbox-hdp.hortonworks.com:9083")
  //    //    .config("hive.metastore.warehouse.dir", "172.168.0.2" + "user/hive/warehouse")
  //    //      .config("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")
  //    .enableHiveSupport()
  //    .getOrCreate()
  //
  //    //StreamingExamples.setStreamingLogLevels()
  //    val sparkConf = sparkSession.sparkContext.getConf
  //
  //    // Create the context
  //    val ssc = new StreamingContext(sparkConf, Seconds(30))
  //
  //
  //    val dfHive = sc.sql("SELECT * from default.table_test")
  //
  //    // Create the FileInputDStream on the directory and use the
  //    // stream to count words in new files created
  //    val lines = ssc.textFileStream(dfHive.toString())
  //    val words = lines.flatMap(_.split(" "))
  //    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  //    wordCounts.print()
  //    ssc.start()
  //    ssc.awaitTermination()
  //
  //
  //
  //
  //
  //
  //
  //}
}
