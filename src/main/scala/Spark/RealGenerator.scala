package Spark

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    .setMaster("local")
  val sc = new SparkContext(cc)
  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("generateDataRATP")
    .config("hive.metastore.uri", "thrift://sandbox-hdp.hortonworks.com:9083")
    .enableHiveSupport()
    .getOrCreate()

    sparkSession.sql(
    "CREATE TABLE IF NOT EXISTS default.table_gen (" +
      "id_titre Int, " +
      "id_lecteur String, " +
      "id_function String, " +
      "id_jour Int, " +
      "id_tranche Int, " +
      "h_validation String, " +
      "nbr_validation Int " +
      ")"
  )

  val sample = sparkSession.sql("DESCRIBE default.table_gen").collect()
  sample.foreach(println)


  sparkSession.stop()
}