ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "TextAnalysis"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "com.crealytics" %% "spark-excel" % "3.3.1_0.18.5",
  "org.jsoup" % "jsoup" % "1.15.4",
  "org.scalaj" %% "scalaj-http" % "2.4.2"
)