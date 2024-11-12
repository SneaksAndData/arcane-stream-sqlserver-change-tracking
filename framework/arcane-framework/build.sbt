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

    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-api
    libraryDependencies += "org.apache.iceberg" % "iceberg-api" % "1.7.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-core
    libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "1.7.0" % "runtime",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-parquet
    libraryDependencies += "org.apache.iceberg" % "iceberg-parquet" % "1.7.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws
    libraryDependencies += "org.apache.iceberg" % "iceberg-aws" % "1.7.0",

        // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % Test

  )
