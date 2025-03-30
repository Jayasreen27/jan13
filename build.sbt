version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.11"

lazy val root = (project in file("."))
  .settings(
    name := "untitled"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"