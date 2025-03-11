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

    assembly / mainClass := Some("com.sneaksanddata.arcane.sql_server_change_tracking.main"),

    // We do not use the version name here, because it's executable file name
    // and we want to keep it consistent with the name of the project
    assembly / assemblyJarName := "com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar",

    assembly / assemblyMergeStrategy := {
        // Removes duplicate files from META-INF
        // Mostly io.netty.versions.properties, license files, INDEX.LIST, MANIFEST.MF, etc.
        case "NOTICE" => MergeStrategy.discard
        case "LICENSE" => MergeStrategy.discard
        case ps if ps.startsWith("META-INF") => MergeStrategy.discard
        case ps if ps.endsWith("module-info.class") => MergeStrategy.discard
        case ps if ps.endsWith("package-info.class") => MergeStrategy.discard

        // for javax.activation package take the first one
        case PathList("javax", "activation", _*) => MergeStrategy.first

        // For other files we use the default strategy (deduplicate)
        case x => MergeStrategy.deduplicate
    }
  )
  .dependsOn(ProjectRef(file("../../framework/arcane-framework"), "root"))
