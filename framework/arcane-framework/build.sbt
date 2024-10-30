val scala361 = "3.6.1"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := scala361

lazy val root = (project in file("."))
  .settings(
    name := "arcane-framework",
    idePackagePrefix := Some("com.sneaksanddata.arcane.framework"),

    // Compiler options
    Test / logBuffered := false,

    // Framework dependencies
    libraryDependencies += "io.delta" % "delta-kernel-api" % "4.0.0rc1",
    libraryDependencies += "dev.zio" %% "zio" % "2.1.6",
    libraryDependencies += "dev.zio" %% "zio-streams" % "2.1.6",
    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11",
    libraryDependencies += "software.amazon.awssdk" % "s3" % "2.25.27",

    libraryDependencies += "org.apache.parquet" % "parquet" % "1.14.1",
    libraryDependencies += "org.apache.parquet" % "parquet-format-structures" % "1.14.1",
    libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.14.1",

    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % Test

  )
