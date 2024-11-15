import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / trackInternalDependencies := TrackLevel.TrackIfMissing
ThisBuild / exportJars := true
ThisBuild / scalaVersion := "3.6.1"



lazy val plugin = (project in file("."))
  .settings(
    name := "arcane-stream-sqlserver-change-tracking",
    idePackagePrefix := Some("com.sneaksanddata.arcane.sql_server_change_tracking"),
    libraryDependencies += "dev.zio" %% "zio-json" % "0.6.2",
    libraryDependencies += "dev.zio" %% "zio-logging" % "2.3.0",
    libraryDependencies += "dev.zio" %% "zio-logging-slf4j" % "2.2.4",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"
  )
  .dependsOn(ProjectRef(file("../../framework/arcane-framework"), "root"))
