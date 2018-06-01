package Spark

import java.text.SimpleDateFormat
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.time.temporal.ChronoUnit.DAYS

import com.google.common.util.concurrent.RateLimiter

import scala.collection.mutable.ListBuffer



/**
  * sudo scp -i ~/.ssh/talan_key -o GSSAPIAuthentication=yes  GeneratedDataBenchmark.jar user@167.114.227.216:/home/user/fehri/
  * /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --class Spark.GenerateHiveData generateddatabenchmark_2.11-0.1.jar
  */

object GenerateHiveData extends App{

  override def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession //initiation de spark et de tout les outils necessaires
      .builder()
      .master("local")
      .appName("generateData")
      .config("hive.metastore.uris", "thrift://hadoop-c2.talan:9083")
      .config("spark.sql.warehouse.dir", "/Users/ihebfehri/Desktop/GeneratedDataBenchmark/src/main/scala/Spark/utils/hive-site.xml")
      .enableHiveSupport()
      .getOrCreate()


    // Controlleur de flux 1.0x/seconde
    val rateLimiter = RateLimiter.create(1.0)

    //fonction en boucle
    while ({true}) {
      rateLimiter.acquire() //lui associer au controleur de flux
      println(" ----------  generating 1000 rows. ----------")
      generateRandomData(1000, sparkSession, args(0)) //fonction qui va me generer 1000 lignes et les ecrire dans Hive
    }
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

    sparkSession.stop()  // lui dire que c'est la fin
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

  def generateRandomData(numberofLines : Int, scc: SparkSession, args : String): Unit = {
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
    df.write.format("hive").mode("append").saveAsTable("default."+ args(0))
  }
}


////JE SAIS PAS ENCORE
//class RateLimiter(delayMs: Int) {
//  private[this] var last: Long = System.currentTimeMillis
//  private[this] val done = scala.actors.Futures.alarm(0)
//  def request = {
//    val now = System.currentTimeMillis
//    val elapsed = synchronized {
//      val elapsed = now - last
//      last = if (elapsed < delayMs) last+delayMs else now
//      elapsed
//    }
//    if (elapsed < delayMs) scala.actors.Futures.alarm(delayMs-elapsed)
//    else done
//  }
//}
