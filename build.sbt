ThisBuild / version := "0.0.1"
name := "diane"
organization := "com.brayanjuls"

scalaVersion := "2.12.12"


libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided",
    "org.apache.spark" %% "spark-hive" % "3.3.1" % "provided",
    "io.delta" %% "delta-core" % "2.1.0" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.lihaoyi" %% "os-lib" % "0.7.1" % "test"
)


Test / fork := true
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
