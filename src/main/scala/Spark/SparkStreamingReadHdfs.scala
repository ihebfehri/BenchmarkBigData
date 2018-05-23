package Spark

object SparkStreamingReadHdfs {
  import org.apache.spark.SparkConf
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.StreamingContext._
  import org.apache.spark._
  //import java.util.stream.StreamSpliterators.ArrayBuffer
  import scala.collection.mutable.ArrayBuffer

//  def main(args: Array[String]) {
//
//        System.setProperty("hadoop.home.dir", "c://winutil//")
//        val conf = new SparkConf().setAppName("Application").setMaster("local[2]")
//        //val sc = new SparkContext(conf)
//        val ssc = new StreamingContext(conf,Seconds(30))
//
//        val input=ssc.textFileStream("hdfs://hadoop-c1.talan:8020/apps/hive/warehouse/table_test")
//
//
//        print("------ Starting Analysis!!! -------")
//        val lines=input.flatMap(_.split(" "))
//        val words=lines.map(word=>(word,1))
//        val counts=words.reduceByKey(_+_)
//        counts.print()
//        val arr = new ArrayBuffer[String]();
//        ssc.start()
//        ssc.awaitTermination()
//
//      }
//
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkStreamingReadHdfs <directory>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
}
}
