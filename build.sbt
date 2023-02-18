ThisBuild / version := "0.0.1"
name := "diane"
organization := "com.brayanjuls"

scalaVersion := "2.12.12"

val sparkVersion= "3.3.1"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "io.delta" %% "delta-core" % "2.1.0" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.lihaoyi" %% "os-lib" % "0.7.1" % "test"
)


Test / fork := true
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
