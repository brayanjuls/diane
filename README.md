# Diane

This library provides helpful Hive helper functions for apache spark users. 

![images](image/diane.png)

## Setup
```scala
libraryDependencies += "com.brayanjules" %% "diane" % "0.0.3"
```
You can find the diane releases for different Scala versions:

[Scala version 2.12](https://repo1.maven.org/maven2/com/brayanjules/diane_2.12/)

[Scala version 2.13](https://repo1.maven.org/maven2/com/brayanjules/diane_2.13/)


## Function Documentation

### Create View
This function `createOrReplaceHiveView` creates a hive view from a delta table. The View will contain all the columns
of the delta table, meaning that it will be like coping the table to a view not filtering or transformations are possible.

Here's how to use the function:
```scala
HiveHelpers.createOrReplaceHiveView(viewName = "students",deltaPath = "file:/path/to/your/delta-lake/table",deltaVersion = 100L)
```

Note that this function will create the hive view based on a specific version of the delta table.

### Get Table Type
The function `getTableType` return the table type(Managed, External or Non-registered) of a given table name. The
return type is a enum value containing the label string.

Here's how to use the function:
```scala
HiveHelpers.getTableType(tableName = "students")
```
The result will be an HiveTableType:

```scala
HiveTableType.EXTERNAL(label = "EXTERNAL")
```

### Register a Parquet or Delta table to Hive
The function `registerTable` adds metadata information of a parquet or delta table to the Hive metastore,
this enables it to be queried.

Here is how to use the function:
```scala
HiveHelpers.registerTable(tableName = "students",tableLoc = "file:/path/to/your/table", provider = HiveProvider.DELTA)
```
after that you would be able to query, i.e:
```scala
SparkSession.active.sql("select * from students").show
```

## How to contribute
We welcome contributions to this project, to contribute checkout our [CONTRIBUTING.md](CONTRIBUTING.md) file.

## How to build the project

### pre-requisites
* SBT 1.8.2
* Java 8
* Scala 2.12.12

### Building

To compile, run
`sbt compile`

To test, run
`sbt test`

To generate artifacts, run
`sbt package`