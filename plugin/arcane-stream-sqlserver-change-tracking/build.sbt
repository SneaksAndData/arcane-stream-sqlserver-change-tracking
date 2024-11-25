import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / trackInternalDependencies := TrackLevel.TrackIfMissing
ThisBuild / exportJars := true
ThisBuild / scalaVersion := "3.6.1"
ThisBuild / organization := "com.sneaksanddata"



lazy val plugin = (project in file("."))
  .settings(
    name := "arcane-stream-sqlserver-change-tracking",
    idePackagePrefix := Some("com.sneaksanddata.arcane.sql_server_change_tracking"),
    libraryDependencies += "dev.zio" %% "zio-json" % "0.6.2",
    libraryDependencies += "dev.zio" %% "zio-logging" % "2.3.0",
    libraryDependencies += "dev.zio" %% "zio-logging-slf4j" % "2.2.4",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36",
    assembly / mainClass := Some("com.sneaksanddata.arcane.sql_server_change_tracking.main"),

    // We do not use the version name here, because it's executable file name
    // and we want to keep it consistent with the name of the project
    assembly / assemblyJarName := "com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar",

    assembly / assemblyMergeStrategy := {
        // Removes duplicate files from META-INF
        // Mostly io.netty.versions.properties, license files, INDEX.LIST, MANIFEST.MF, etc.
        case PathList("META-INF", _) => MergeStrategy.discard

        // For other files we use the default strategy (deduplicate)
        case x => MergeStrategy.deduplicate
    }
  )
  .dependsOn(ProjectRef(file("../../framework/arcane-framework"), "root"))
