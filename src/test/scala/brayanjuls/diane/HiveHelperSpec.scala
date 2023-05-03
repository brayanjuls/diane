package brayanjuls.diane

import brayanjuls.diane.HiveHelpers.HiveTableType
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.BeforeAndAfterEach

class HiveHelperSpec extends AnyFunSpec with SparkSessionTestWrapper with BeforeAndAfterEach {
  import spark.implicits._
  override def afterEach(): Unit = {
    val tmpDir = os.pwd / "tmp"
    os.remove.all(tmpDir)
    SparkSession.active.sql("drop table IF EXISTS num_table")
  }

  describe("Hive table types") {
    it("should return table type managed") {

      val df        = List("1", "2", "3").toDF
      val tableName = "num_table"
      df.write.saveAsTable(tableName)
      val result   = HiveHelpers.getTableType(tableName)
      val expected = HiveTableType.MANAGED
      assertResult(expected)(result)
    }

    it("should return table type external") {
      val df        = List("1", "2", "3").toDF
      val tableName = "num_table"
      val tmpDir    = os.pwd / "tmp"
      val dataDir   = tmpDir / tableName / ".parquet"
      df.write.save(tmpDir.toString)
      SparkSession.active.sql(
        s"CREATE EXTERNAL TABLE num_table(value string) STORED AS PARQUET LOCATION '$dataDir'"
      )
      val result   = HiveHelpers.getTableType(tableName)
      val expected = HiveTableType.EXTERNAL
      assertResult(expected)(result)
    }

    it("should return table type non-registered") {
      val tableName = "num_table"
      val result    = HiveHelpers.getTableType(tableName)
      val expected  = HiveTableType.NONREGISTERED
      assertResult(expected)(result)
    }

    it("should be able to recognize a managed delta table") {
      val df        = List("1", "2", "3").toDF
      val tableName = "num_table"
      df.write
        .format("delta")
        .saveAsTable(tableName)
      val result   = HiveHelpers.getTableType(tableName)
      val expected = HiveTableType.MANAGED
      assertResult(expected)(result)
    }

    it("should be able to recognize an external delta table") {
      val df        = List("1", "2", "3").toDF
      val tableName = "num_table"
      val tmpDir    = os.pwd / "tmp"
      df.write
        .format("delta")
        .option("path", tmpDir.toString())
        .saveAsTable(tableName)
      val result   = HiveHelpers.getTableType(tableName)
      val expected = HiveTableType.EXTERNAL
      assertResult(expected)(result)
    }

    it("should be able to recognize an non-registered delta table") {
      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      df.write
        .format("delta")
        .save((tmpDir / tableName).toString())
      val result   = HiveHelpers.getTableType(tableName)
      val expected = HiveTableType.NONREGISTERED
      assertResult(expected)(result)
    }
  }

  describe("Register a table to hive") {
    it("should register a delta table to hive") {
      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      val tableLoc  = (tmpDir / tableName).toString()
      df.write
        .format("delta")
        .save(tableLoc)
      assertThrows[AnalysisException] {
        spark.sql(s"DESCRIBE table $tableName")
      }
      HiveHelpers.registerTable(tableName, tableLoc)
      val dfDescribe = spark.sql(s"DESCRIBE table EXTENDED $tableName")
      assert(dfDescribe.count() > 0)
    }

    it("should register parquet table to hive") {
      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      val tableLoc  = (tmpDir / tableName).toString()
      df.write
        .format("parquet")
        .save(tableLoc)
      HiveHelpers.registerTable(s"$tableName", tableLoc, HiveHelpers.HiveProvider.PARQUET)
      val dfDescribe = SparkSession.active.sql(s"describe extended $tableName")
      assert(dfDescribe.count() > 0)
    }

    it("should fail to register an already registered table to hive") {
      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      val tableLoc  = (tmpDir / tableName).toString()
      df.write
        .format("delta")
        .saveAsTable(tableName)

      val errorMessage = intercept[DianeValidationError] {
        HiveHelpers.registerTable(tableName, tableLoc)
      }.getMessage
      val expected = s"table:$tableName already exits"
      assertResult(expected)(errorMessage)
    }

    it("should fail to register when the file path is empty") {
      val tableName = "num_table"
      val tableLoc  = ""
      val errorMessage = intercept[DianeValidationError] {
        HiveHelpers.registerTable(tableName, tableLoc)
      }.getMessage
      val expected = "tableName and tablePath input parameters must not be empty"
      assertResult(expected)(errorMessage)
    }

    it("should fail when the wrong provider is specified") {
      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      val tableLoc  = (tmpDir / tableName).toString()
      df.write
        .format("parquet")
        .save(tableLoc)
      val errorMessage = intercept[DianeValidationError] {
        HiveHelpers.registerTable(s"$tableName", tableLoc, HiveHelpers.HiveProvider.DELTA)
      }.getMessage
      val expected = s"table:$tableName location:$tableLoc is not a delta table"
      assertResult(expected)(errorMessage)
    }
  }

  describe("create or replace view from delta table") {
    it("should successful create a hive view") {

      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      val tableLoc  = (tmpDir / tableName).toString()
      df.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
      val viewName = "view_num_table"
      HiveHelpers.createOrReplaceHiveView(viewName, tableLoc, 0)
      val result = SparkSession.active.sql(s"select * from $viewName").count()
      assertResult(3)(result)
    }

    it("should fail to create a hive view when the table path is not valid") {
      val df        = List("1", "2", "3").toDF
      val tmpDir    = os.pwd / "tmp"
      val tableName = "num_table"
      val tableLoc  = (tmpDir / tableName).toString()
      df.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
      val viewName = "view_num_table"
      val errorMessage = intercept[AnalysisException] {
        HiveHelpers.createOrReplaceHiveView(viewName, "path/to/non_existing_table", 0)
      }.getMessage()

      assertResult("Unsupported data source type for direct query on files: delta; line 3 pos 23")(
        errorMessage
      )
    }
  }

  describe("Show all objects in the metadata db") {

    it("should return an empty dataframe") {
      val tableNames = HiveHelpers.allTables().collect().map(t => t.getAs[String]("tableName"))
      assert(tableNames.isEmpty)
    }

    it("should return all delta and parquet tables in the metastore using default database") {
      val tableName = "lang_num_table"
      val tmpDir    = os.pwd / "tmp"
      val df =
        List(("1", "one"), ("2", "two"), ("3", "three"), ("1", "uno")).toDF("num", "description")
      val df2 = List("1", "2", "3", "4").toDF()

      df.write
        .partitionBy("num")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)

      df.write
        .partitionBy("num")
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("path", (tmpDir / "e_new_table").toString())
        .saveAsTable("e_new_table")

      df2.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable("num_table")

      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", (tmpDir / "p_e_new_table").toString())
        .saveAsTable("p_e_new_table")


          val df_inventory = List(
            (1, "eggs","eggs",1030,3.4,334,2),
            (2, "tuna","fish tuna",850,5.0,367,2),
            (3, "pineapple","fruit",101,2.5,12,2),
            (4, "potatoes","vegetable",3999,3.4,367,2),
          ).toDF("id","name","description","quantity","price","vendor_id","category_id")

          df_inventory.write
            .partitionBy("category_id")
            .format("delta")
            .mode(SaveMode.Overwrite)
            .option("path", (tmpDir / "inventory").toString())
            .saveAsTable("inventory")


      df.createOrReplaceTempView("tmp_num_view")
      HiveHelpers.createOrReplaceHiveView("pem_num_view", (tmpDir / "num_table").toString, 0)

      val tableNames = HiveHelpers.allTables().collect().map(t => t.getAs[String]("tableName"))

      assertResult(Seq("e_new_table", "inventory", "lang_num_table", "num_table", "p_e_new_table"))(tableNames)
    }

    it("should return all delta and parquet tables filtered by database") {
      val df = List("1", "2", "3", "4").toDF()
      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("num_table")

      spark.sql("CREATE DATABASE IF NOT EXISTS hive_testing;")

      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("hive_testing.num_table_2")


      val tableNames = HiveHelpers.allTables("hive_testing").collect().map(t => t.getAs[String]("tableName"))
      assertResult(Seq("num_table_2"))(tableNames)
      spark.sql("DROP DATABASE IF EXISTS hive_testing CASCADE;")
    }

    it("should fail when the database specificed does not exits"){
      val databaseName = "non_existing_db"
      val  messageException = intercept[DianeValidationError] {
        HiveHelpers.allTables(databaseName)
      }.getMessage

      assertResult(s"Database '$databaseName' not found")(messageException)
    }

    it("should describe only the provided tables"){
      val tableName = "lang_num_table"
      val tmpDir = os.pwd / "tmp"
      val df =
        List(("1", "one"), ("2", "two"), ("3", "three"), ("1", "uno")).toDF("num", "description")
      val df2 = List("1", "2", "3", "4").toDF()

      df.write
        .partitionBy("num")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)

      df.write
        .partitionBy("num")
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("path", (tmpDir / "e_new_table").toString())
        .saveAsTable("e_new_table")

      df2.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable("num_table")

      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", (tmpDir / "p_e_new_table").toString())
        .saveAsTable("p_e_new_table")

      val tableNames = HiveHelpers
        .allTables("default",Seq("e_new_table"))
        .collect().map(t => t.getAs[String]("tableName"))

      assertResult(Seq("e_new_table"))(tableNames)
    }
  }
}
