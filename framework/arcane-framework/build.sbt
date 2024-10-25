ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.1"

lazy val root = (project in file("."))
  .settings(
    name := "arcane-framework",
    idePackagePrefix := Some("com.sneaksanddata.arcane.framework"),
    libraryDependencies += "io.delta" % "delta-kernel-api" % "4.0.0rc1",
    libraryDependencies += "dev.zio" %% "zio" % "2.1.6",
    libraryDependencies += "dev.zio" %% "zio-streams" % "2.1.6",
  )
