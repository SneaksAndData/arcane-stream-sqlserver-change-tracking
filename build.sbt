import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / trackInternalDependencies := TrackLevel.TrackIfMissing
ThisBuild / exportJars := true
ThisBuild / scalaVersion := "3.6.1"
ThisBuild / organization := "com.sneaksanddata"

resolvers += "Arcane framework repo" at "https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala"

credentials += Credentials(
    "GitHub Package Registry",
    "maven.pkg.github.com",
    "_",
    sys.env("GITHUB_TOKEN")
)

mainClass := Some("com.sneaksanddata.arcane.sql_server_change_tracking.main")

lazy val plugin = (project in file("."))
  .settings(
    name := "arcane-stream-sqlserver-change-tracking",
    idePackagePrefix := Some("com.sneaksanddata.arcane.sql_server_change_tracking"),
    libraryDependencies += "com.sneaksanddata" % "arcane-framework_3" % "1.2.4-21-g7647e4b",
    libraryDependencies += "io.netty" % "netty-tcnative-boringssl-static" % "2.0.74.Final",

    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % Test,
    libraryDependencies += "dev.zio" %% "zio-test"          % "2.1.24" % Test,
    libraryDependencies += "dev.zio" %% "zio-test-sbt"      % "2.1.24" % Test,

    Test / parallelExecution := false,

    assembly / mainClass := Some("com.sneaksanddata.arcane.sql_server_change_tracking.main"),

    // We do not use the version name here, because it's executable file name
    // and we want to keep it consistent with the name of the project
    assembly / assemblyJarName := "com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar",

    assembly / assemblyMergeStrategy := {
        case "NOTICE" => MergeStrategy.discard
        case "LICENSE" => MergeStrategy.discard
        case ps if ps.contains("META-INF/services/java.net.spi.InetAddressResolverProvider") => MergeStrategy.discard
        case ps if ps.contains("META-INF/services/") => MergeStrategy.concat("\n")
        case ps if ps.startsWith("META-INF/native") => MergeStrategy.first

        // Removes duplicate files from META-INF
        // Mostly io.netty.versions.properties, license files, INDEX.LIST, MANIFEST.MF, etc.
        case ps if ps.startsWith("META-INF") => MergeStrategy.discard
        case ps if ps.endsWith("logback.xml") => MergeStrategy.discard
        case ps if ps.endsWith("module-info.class") => MergeStrategy.discard
        case ps if ps.endsWith("package-info.class") => MergeStrategy.discard

        // for javax.activation package take the first one
        case PathList("javax", "activation", _*) => MergeStrategy.last
        case PathList("javax", "xml", _*) => MergeStrategy.last

        // For other files we use the default strategy (deduplicate)
        case x => MergeStrategy.deduplicate
    }
  )
