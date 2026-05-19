# SQL Server Change Tracking stream for Arcane Data Platform
<img src="docs/images/arcane-logo.png" width="100" height="100" alt="logo"> 

![Static Badge](https://img.shields.io/badge/Scala-3-red)
[![Run tests with coverage](https://github.com/SneaksAndData/arcane-stream-sqlserver-change-tracking/actions/workflows/build.yaml/badge.svg)](https://github.com/SneaksAndData/arcane-stream-sqlserver-change-tracking/actions/workflows/build.yaml)

This repository will contain the SQL Server Change Tracking stream for the Arcane. This stream will allow you to
capture changes from SQL Server database and store them in a S3 compatible storage.

### Quickstart

TBD

### Development setup
#### Installing JAVA
A few suggested ways to install/manage JAVA and its versions:
- SDKMAN (https://sdkman.io/):
  - List available JAVA versions: `sdk list java`
  - Installing JAVA with specific version/vendor: `sdk install java 21.0.2-tem`
- mise (https://mise.jdx.dev/):
  - List available JAVA versions: `mise ls-remote java`
  - Installing JAVA with specific version/vendor: `mise use -g java@temurin-21.0.2+13.0.LTS`

#### Installing SBT
A few suggested ways to install/manage SBT and its versions:
- SDKMAN (https://sdkman.io/):
    - List available JAVA versions: `sdk list sbt`
    - Installing JAVA with specific version/vendor: `sdk install sbt 1.10.1`
- mise (https://mise.jdx.dev/):
    - List available JAVA versions: `mise ls-remote java`
    - Installing JAVA with specific version/vendor: `mise use -g sbt@1.10.1`

#### Getting access to GitHub Packages registry
In order to build, test and run the project, `GITHUB_TOKEN` environment variable needs to be set.
It is used to authenticate against GitHub Maven package registry, specifially for JAR dependencies under
https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala.

Create [new](https://github.com/settings/personal-access-tokens/new) personal access token PAT (Personal Access Token).
For example, fine-grained token with "Public repositories" access and without explicit permissions.

Export `GITHUB_TOKEN` environment variable before running any `sbt` commands.
For example, put `export GITHUB_TOKEN=github_pat_xxx` line in your `.zshrc`/`.bashrc` file.

#### Building the project
To build a fat JAR run `sbt assembly`.

#### Running tests

#### Running plugin locally

### Development

Project uses `Scala 3.6.1` and tested on JDK 21. When using GraalVM, use JDK 22 version.

Plugin supports `GraalVM` native image builds. Each PR must be run with 
`-agentlib:native-image-agent=config-merge-dir=./configs` on a [GraalVM-CE JDK](https://sdkman.io/jdks/#graalce) in 
order to capture native image settings updates, if any.

