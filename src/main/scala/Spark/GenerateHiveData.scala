package Spark

import java.text.SimpleDateFormat
import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.time.temporal.ChronoUnit.DAYS

import scala.collection.mutable.ListBuffer



/**
  * sudo scp -i ~/.ssh/talan_key -o GSSAPIAuthentication=yes  GeneratedDataBenchmark.jar user@167.114.227.216:/home/user/fehri/
  * /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --class Spark.RealGenerator generateddatabenchmark_2.11-0.1.jar
  */

object GenerateHiveData extends App{

  override def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
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
    import sparkSession.implicits._

    generateRandomData(1000, sparkSession)

    //
//        sparkSession.sql(
//          "CREATE TABLE IF NOT EXISTS default.table_gen (" +
//            "id_titreTransport Int, " + //
//            "id_lecteurCarte String, " +
//            "id_type_Transport String, " +
//            "id_date Int, " +
//            "id_trancheHoraire Int, " +
//            "h_validation String, " +
//            "nbr_validation Int " +
//            ")"
//        )
    //val sample = sparkSession.sql("DESCRIBE default.employee").collect()
    //    sample.foreach(println)

    sparkSession.stop()
  }

//  def randomDateFromRange(from: String, to: String): String = {
//    val diff = DAYS.between(from, to)
//    val random = new Random(System.nanoTime) // You may want a different se
//    // ed
//    from.plusDays(random.nextInt(diff.toInt))
//  }

  def randomTimeFromRange(): String = {
    val r = scala.util.Random

    // create the date/time formatters
    val minuteFormat = new SimpleDateFormat("mm")
    val hourFormat = new SimpleDateFormat("hh")

    val currentHour = hourFormat.format(r.nextInt(23))      // 12
    val currentMinute = minuteFormat.format(r.nextInt(59))  // 29

    "%s:%s".format(currentHour, currentMinute)
  }

  var rowList: (Int, String, String, Int, Int, String, Int) = _


  def generateRandomData(numberofLines : Int, scc: SparkSession): Unit = {
    var finalRowList=  new ListBuffer[(Int, String, String, Int, Int, String, Int)]()
    val listTransport = Array("Bus", "Metro", "RER", "Tram", "Bateau", "Bus_Nuit")
    val r = scala.util.Random

    for (cpt <- 1 to numberofLines) {
      rowList = (
//        cpt,
        r.nextInt(100),  //int
        r.alphanumeric.take(10).mkString,  //string
        listTransport(r.nextInt(5)), //string
        r.nextInt(31),   // int
        r.nextInt(100),
//        randomDateFromRange(String.of(2017,1,1), String.of(2017,12,30)).toString , //localdate
        randomTimeFromRange(),
        r.nextInt(5)+1 //int
      )
      finalRowList.append(rowList)
    }

    val rdd = scc.sparkContext.parallelize(finalRowList)
    val df = scc.createDataFrame(rdd).toDF("id_titretransport", "id_lecteurcarte", "id_type_transport", "id_date", "id_tranchehoraire", "h_validation", "nbr_validation")
    df.write.format("hive").mode("append").saveAsTable("default.table_test")
    //todo input stream instead of write.mode above
    //todo spark stream the data from hive
  }



}