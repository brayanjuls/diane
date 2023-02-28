package brayanjuls.diane

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.functions.{array, col, element_at, first, lit, map, map_concat, split, struct, typedLit, udf, when}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Encoders, SparkSession, functions}

import scala.Seq

object HiveHelpers {

  def createOrReplaceHiveView(viewName: String, deltaPath: String, deltaVersion: Long): Unit = {
    val query = s"""
      CREATE OR REPLACE VIEW $viewName
      AS SELECT * FROM delta.`$deltaPath@v$deltaVersion`
    """.stripMargin
    SparkSession.active.sql(query)
  }

  def getTableType(tableName: String): HiveTableType = {
    try {

      val table = SparkSession.active.catalog.getTable(tableName)

      table.tableType.toUpperCase() match {
        case HiveTableType.MANAGED.label  => HiveTableType.MANAGED
        case HiveTableType.EXTERNAL.label => HiveTableType.EXTERNAL
      }
    } catch {
      case e: AnalysisException
          if e.getMessage().toLowerCase().contains(s"table or view '$tableName' not found") =>
        HiveTableType.NONREGISTERED
    }
  }

  def registerTable(
      tableName: String,
      tablePath: String,
      provider: HiveProvider = HiveProvider.DELTA
  ): Unit = {
    if (tablePath.isEmpty || tableName.isEmpty) {
      throw DianeValidationError("tableName and tablePath input parameters must not be empty")
    }
    try {
      if (provider == HiveProvider.DELTA) {
        SparkSession.active.sql(s"CREATE TABLE $tableName using delta location '$tablePath'")
      } else {
        SparkSession.active.catalog.createTable(tableName, tablePath)
      }

    } catch {
      case e: DeltaAnalysisException =>
        throw DianeValidationError(s"table:$tableName location:$tablePath is not a delta table")
      case e: TableAlreadyExistsException =>
        throw DianeValidationError(s"table:$tableName already exits")
    }
  }

  def allTables(): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._
    val catalog   = SparkSession.active.catalog
    val allTables = catalog.listTables()

    val tableDetailDF = allTables
      .collect()
      .filter(t => t.tableType.equalsIgnoreCase(HiveTableType.MANAGED.label) ||
        t.tableType.equalsIgnoreCase(HiveTableType.EXTERNAL.label))
      .map(t =>
        spark
          .sql(s"DESCRIBE TABLE EXTENDED ${t.name};")
          .groupBy()
          .pivot("col_name")
          .agg(first("data_type"))
          .withColumn(
            "partitionColumns",
            typedLit(
              spark.catalog
                .listColumns(t.name)
                .where($"ispartition" === true)
                .select("name")
                .collect()
                .map(_.getAs[String]("name"))
            )
          )
          .withColumn("bucketColumns",
            typedLit(
              spark.catalog
                .listColumns(t.name)
                .where($"isbucket" === true)
                .select("name")
                .collect()
                .map(_.getAs[String]("name"))
            )
          )
          .withColumn("type",lit(t.tableType))
      )

    val resultColumnNames = Seq("database", "tableName", "provider", "owner", "partitionColumns","bucketColumns", "type", "detail")
    /**
     * The `if` conditions inside the `when` functions are needed because the sql sentence "describe table ..." return
     * different columns for each provider, and if you use the name of a column without the if condition the spark query parser
     * will throw an exception if any of the columns do not exist in the current dataframe even though the condition in the
     * `when` function evaluate to false.
     */
    def setColumns(df: DataFrame) = {
      df
        .withColumn("provider", $"Provider")
        .withColumn("owner", $"Owner")
        .withColumn(
          "tableName",
          when(
            $"provider" === lit("delta"),
            if (df.columns.contains("Name")) split($"Name", "\\.").getItem(1) else lit("N/A")
          )
            .when(
              $"provider" === lit("parquet"),
              if (df.columns.contains("Table")) $"Table" else lit("N/A")
            )
            .otherwise(lit("N/A"))
        )
        .withColumn(
          "database",
          when(
            $"provider" === lit("delta"),
            if (df.columns.contains("Name")) split($"Name", "\\.").getItem(0) else lit("N/A")
          )
            .when(
              $"provider" === lit("parquet"),
              if (df.columns.contains("Database")) $"Database" else lit("N/A")
            )
            .otherwise(lit("N/A"))
        )
        .withColumn(
          "detail",
          when(
            $"provider" === lit("delta"),
            if (df.columns.contains("Table Properties"))
              map(lit("tableProperties"), $"Table Properties")
            else map()
          )
            .when(
              $"provider" === lit("parquet"),
              if (df.columns.toSeq.intersect(Seq("InputFormat", "OutputFormat")).size == 2)
                map_concat(
                  map(lit("inputFormat"), $"InputFormat"),
                  map(lit("outputFormat"), $"OutputFormat")
                )
              else map()
            )
            .otherwise(map())
        )
        .select(resultColumnNames.map(col):_*)
    }

    val emptyDF = Seq.empty[(String,String,String,String,Array[String],Array[String],String,Map[String,String])]
      .toDF(resultColumnNames:_*)

    val singleDFDetail = tableDetailDF
      .map(df => df.transform(setColumns))
      .fold(emptyDF)((df1, df2) => df1.union(df2))

    singleDFDetail
  }

  sealed abstract class HiveTableType(val label: String)

  sealed abstract class HiveProvider(val label: String)

  object HiveProvider {
    final case object DELTA   extends HiveProvider(label = "delta")
    final case object PARQUET extends HiveProvider(label = "parquet")
  }

  object HiveTableType {
    final case object MANAGED extends HiveTableType(label = "MANAGED")

    final case object EXTERNAL extends HiveTableType(label = "EXTERNAL")

    final case object NONREGISTERED extends HiveTableType(label = "NONREGISTERED")
  }
}

case class DianeValidationError(smth: String, e: Throwable = new Exception())
    extends Exception(smth, e)
