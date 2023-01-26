package brayanjuls.diane

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession.builder()
      .master("local")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", (os.pwd / "tmp").toString())
      .appName("spark session")
      .enableHiveSupport()
      .getOrCreate()
  }
  spark.sparkContext.setLogLevel(Level.OFF.toString)

}
