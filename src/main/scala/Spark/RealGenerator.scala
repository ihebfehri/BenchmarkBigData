package Spark

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Random}

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.temporal.ChronoUnit.DAYS


/**
  * spark-submit  --master yarn --class Spark.localyGeneratedData localGen.jar "2017" "2018" "100" "ft_validations"
  */

object RealGenerator extends App{

  //  val spark = SparkSession
  //    .builder()
  //    .master("local[*]")
  //    .appName("generateDataRATP")
  //    .config("hive.metastore.uri", "thrift://sandbox-hdp.hortonworks.com:9083")
  //    .enableHiveSupport()
  //    .getOrCreate()
  //  val sc = spark.sparkContext

  val cc = new SparkConf()
    .setAppName("generateDataRATP")
    .setMaster("yarn-client")
  val sc = new SparkContext(cc)

  val sparkSession = SparkSession
    .builder()
    .master("yarn-client")
    .appName("generateDataRATP")
    //    .config("hive.metastore.uri", "thrift://sandbox-hdp.hortonworks.com:9083")
    //    .config("hive.metastore.warehouse.dir", "172.168.0.2" + "user/hive/warehouse")
    //      .config("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  override def main(args: Array[String]): Unit = {
    generateRandomData(1000000)
  }
  // todo: GET THE WAREHOUSE TO WORK GOD DAMN ITS NOT CONNECTING TO REMOTE SERVER
  // waiting for new server........


  //    sparkSession.sql(
  //    "CREATE TABLE IF NOT EXISTS default.table_gen (" +
  //      "id_titreTransport Int, " +        //
  //      "id_lecteurCarte String, " +
  //      "id_type_Transport String, " +
  //      "id_date Int, " +
  //      "id_trancheHoraire Int, " +
  //      "h_validation String, " +
  //      "nbr_validation Int " +
  //      ")"
  //  )
  //
  //check if it can read
  //  val sample = sparkSession.sql("DESCRIBE default.employee").collect()
  //  sample.foreach(println)



  def randomDateFromRange(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime) // You may want a different seed
    from.plusDays(random.nextInt(diff.toInt))
  }

  def randomTimeFromRange(): String = {
    val today = Calendar.getInstance.getTime
    val r = scala.util.Random

    // create the date/time formatters
    val minuteFormat = new SimpleDateFormat("mm")
    val hourFormat = new SimpleDateFormat("hh")

    val currentHour = hourFormat.format(r.nextInt(23))      // 12
    val currentMinute = minuteFormat.format(r.nextInt(59))  // 29

    "%s:%s".format(currentHour, currentMinute)
  }

  var rowList: (Int, Int, String, Any, Int, LocalDate, String, Int) = _
  def generateRandomData(numberofLines : Int): Unit = {
    val listTransport = Array("Bus", "Metro", "RER", "Tram", "Bateau", "Bus_Nuit")
    val r = scala.util.Random

    for (cpt <- 1 to numberofLines) {

      rowList = (
        cpt,
        r.nextInt(100),  //int
        r.alphanumeric.take(10).mkString,  //string
        listTransport(r.nextInt(5)), //string
        r.nextInt(31),   // int
        randomDateFromRange(LocalDate.of(2017,1,1), LocalDate.of(2017,12,30)) , //localdate
        randomTimeFromRange(),
        r.nextInt(5)+1 //int
      )
      println(rowList)
    }
  }

  sparkSession.stop()
}