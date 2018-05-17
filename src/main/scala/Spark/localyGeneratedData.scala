package Spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.sun.rowset.internal.Row
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.spark.sql.hive.HiveContext

/**
  * sudo scp -i ~/.ssh/talan_key -o GSSAPIAuthentication=yes  GeneratedDataBenchmark.jar user@167.114.227.216:/home/user/fehri/
    /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --class Spark.RealGenerator generateddatabenchmark_2.11-0.1.jar
  */

object localyGeneratedData {


  //declaration of global variable and constant
  val conf = new SparkConf()
    .setAppName("generateDataRATP")
    .setMaster("local")

  //.set("spark.local.ip", "172.16.50.129" )




  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("generateDataRATP")
    .config("hive.metastore.uri", "thrift://hadoop-c1.talan:9083")
    .enableHiveSupport()
    .getOrCreate()
  //  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  //  val sqlContext = new org.apache.spark.sql.SQLContext


  //  var randomDate :Date = _
  var rowList :(Int,String,String,Int,Int,String,Int,Int) = _
  val rnd = new scala.util.Random
//  val idLecteurDF: DataFrame = spark.sql("SELECT id_lecteur FROM data_gen")
//  val rowsIdLecteur: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = idLecteurDF.rdd
//  val listIdLecteur: List[sql.Row] = idLecteurDF.rdd.collect().toList
//  val sizeIdLecteur: Int = listIdLecteur.size
  import spark.implicits._

  //declaration of main function
  def main(args: Array[String]) {
    val mainListHive = new ListBuffer[(Int, String, String, Int, Int, String, Int, Int)]()

    for(inc <- 1 to args(2).toDouble.toInt) {
      var randomDate = defineDate(args(0).toInt, args(1).toInt)
      rowList = (
        randomize(1, 29),//idTitre
        Random.alphanumeric.toString(),
//        getRandomIdLecteur(),//idLecteur
        "F" + randomize(1, 6), //idFonction
        new SimpleDateFormat("yyyyMMdd").format(randomDate).toInt, //idJour
        getIdTranche(randomDate), //idTranche
        new SimpleDateFormat("HHmmss").format(randomDate), //h_validations
        new SimpleDateFormat("yyyyMM").format(randomDate).toInt, //mois
        1 //number_validation, fixed to 1
      )
      mainListHive += rowList
      if (inc % 1000000 == 0){
        sendToHive(mainListHive.toList, args(3))
        mainListHive.clear()
      }
    }

    if(!mainListHive.isEmpty) {
      sendToHive(mainListHive.toList, args(3))
    }
    sc.stop()
  }

  def randomize(start:Int, end:Int):Int ={
    start + rnd.nextInt( (end - start) + 1 )
  }

  /*  def getRandomIdLecteur(rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]):String ={
      rows.takeSample(false,1).head.getString(0)
    }*/

//  def getRandomIdLecteur():String = {
//    listIdLecteur(randomize(0,sizeIdLecteur-1)).getString(0)
//  }

  def sendToHive(listToHive:List[(Int, String, String, Int, Int, String, Int, Int)], targetTable:String) ={
    val rdd = sc.parallelize(listToHive)
    val df = rdd.toDF("ID_TITRE", "ID_LECTEUR", "ID_FUNCTION", "ID_JOUR", "ID_TRANCHE", "H_VALIDATION", "MOIS", "NBRE_VALIDATION")
    df.write.mode("append").saveAsTable("test_hive."+targetTable)

  }

  def getIdTranche(date:Date):Int={
    val minutes = new SimpleDateFormat("mm").format(date).toInt
    val hours = new SimpleDateFormat("HH").format(date).toInt
    if (hours >= 3) {
      var idTranche = hours * 2 - 5
      if (minutes > 30) {
        idTranche += 1
      }
      idTranche
    }
    else {
      var idTranche = hours * 2 + 43
      if (minutes > 30) {
        idTranche += 1
      }
      idTranche
    }
  }

  def defineDate(startDate:Int,endDate:Int):Date = {
    val date1 = Calendar.getInstance
    val date2 = Calendar.getInstance
    date1.set(startDate, 1, 1)
    date2.set(endDate, 1, 1)
    randomDateBetween(date1.getTime, date2.getTime)
  }

  def randomDateBetween( firstDate : Date, secondDate : Date) : Date = {
    val ratio = new Random().nextInt(100);
    val difference = secondDate.getTime - firstDate.getTime
    val surplusMillis = (difference * (ratio / 100.0)).asInstanceOf[Long]
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(surplusMillis + firstDate.getTime)
    return cal.getTime()
  }
}
