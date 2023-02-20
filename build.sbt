name := "diane"
organization := "com.brayanjules"

// Dependencies
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

// Release Process
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("brayanjuls", "diane", "Brayan Jules","brayan1213@gmail.com"))

licenses += ("MIT" -> url("http://opensource.org/licenses/MIT"))
publishMavenStyle := true

sonatypeProfileName := "com.brayanjules"
sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
publishTo := sonatypePublishToBundle.value

import ReleaseTransformations._
releaseCrossBuild := false
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // check that there are no SNAPSHOT dependencies
  inquireVersions, // ask user to enter the current and next version
  runClean, // clean
  runTest, // run tests
  setReleaseVersion, // set release version in version.sbt
  commitReleaseVersion, // commit the release version
  tagRelease, // create git tag
  releaseStepCommandAndRemaining("+publishSigned"), // run +publishSigned command to sonatype stage release
  setNextVersion, // set next version in version.sbt
  commitNextVersion, // commit next version
  releaseStepCommand("sonatypeBundleRelease"), // run sonatypeRelease and publish to maven central
  pushChanges // push changes to git
)