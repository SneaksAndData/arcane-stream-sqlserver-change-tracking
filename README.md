# SQL Server Change Tracking stream for Arcane Data Platform
<img src="docs/images/arcane-logo.png" width="100" height="100" alt="logo"> 

![Static Badge](https://img.shields.io/badge/Scala-3-red)
[![Run tests with coverage](https://github.com/SneaksAndData/arcane-stream-sqlserver-change-tracking/actions/workflows/build.yaml/badge.svg)](https://github.com/SneaksAndData/arcane-stream-sqlserver-change-tracking/actions/workflows/build.yaml)

This repository will contain the SQL Server Change Tracking stream for the Arcane. This stream will allow you to
capture changes from SQL Server database and store them in a S3 compatible storage.

### Quickstart

TBD

### Development

Project uses `Scala 3.6.1` and tested on JDK 21. When using GraalVM, use JDK 22 version.

Plugin supports `GraalVM` native image builds. Each PR must be run with 
`-agentlib:native-image-agent=config-merge-dir=./configs` on a [GraalVM-CE JDK](https://sdkman.io/jdks/#graalce) in 
order to capture native image settings updates, if any.

