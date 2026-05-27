# SQL Server Change Tracking stream for Arcane Data Platform
<img src="docs/images/arcane-logo.png" width="100" height="100" alt="logo"> 

![Static Badge](https://img.shields.io/badge/Scala-3-red)
[![Run tests with coverage](https://github.com/SneaksAndData/arcane-stream-sqlserver-change-tracking/actions/workflows/build.yaml/badge.svg)](https://github.com/SneaksAndData/arcane-stream-sqlserver-change-tracking/actions/workflows/build.yaml)

This repository will contain the SQL Server Change Tracking stream for the Arcane. This stream will allow you to
capture changes from SQL Server database and store them in a S3 compatible storage.

### Development setup

#### Tooling
Install the following tools:
- `mise` - for managing tooling versions, environment variables: https://github.com/jdx/mise
- `just` - for orchestrating tasks: https://github.com/casey/just
- Docker/Docker compose - for integration testing: https://www.docker.com/products/docker-desktop/

Once the above are installed, run `mise install`.
It will install other necessary tools (e.g. JDK and SBT) at recommended versions for this project only.

#### Getting access to GitHub Packages registry
In order to build, test and run the project, `GITHUB_TOKEN` environment variable needs to be set.
It is used to authenticate against GitHub Maven package registry, specifially for JAR dependencies under
https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala.

Create [new](https://github.com/settings/personal-access-tokens/new) personal access token PAT (Personal Access Token).
For example, fine-grained token with "Public repositories" access and without explicit permissions.

Export `GITHUB_TOKEN` environment variable before running any `sbt` commands.
For example, put `export GITHUB_TOKEN=github_pat_xxx` line in your `.zshrc`/`.bashrc` file.

#### Common tasks
- Building the project (fat JAR): `just build`
- Running integration tests: `just it`
- Running streaming application locally:
  - via `just stream [--debug]` or `just backfill [--debug]` (backfill mode). **Note**: `dev.env` is required, see `dev.env.example` for an example application configuration.
- Cleaning build artifacts: `just clean`
- Code style check: `just check`

### Arcane operator and streams on Kind
Local K8S cluster (i.e. [Kind](https://github.com/kubernetes-sigs/kind)) can be used to verify that Arcane operator and
its dependencies coming from Helm charts are correctly setup.

Furthermore, Arcane is lightweight enough so that actual streams can be deployed on the local K8S cluster to, for example,
try out or test features in a dev setup.

#### Setting up Kind
Kind itself should be already installed if you ran `mise install`. Steps afters:
1. Create Kind cluster: `kind create cluster --name arcane-dev`
2. Create namespace: `kubectl create namespace arcane --context kind-arcane-dev`
3. Install required [CRDs](github.com/SneaksAndData/arcane-crd):
```sh
  helm install arcane-crd oci://ghcr.io/sneaksanddata/helm/arcane-crd \
    --version vX.Y.Z \
    --namespace arcane \
    --kube-context kind-arcane-dev
  ```
4. Install Arcane [operator](github.com/SneaksAndData/arcane-operator):
```sh
  helm install arcane oci://ghcr.io/sneaksanddata/helm/arcane-operator \
    --version vX.Y.Z \
    --namespace arcane \
    --kube-context kind-arcane-dev
  ```
5. Install chart from this project:
```sh
  helm upgrade --install arcane-mssql ./.helm \
      --kube-context kind-arcane-dev \
      --namespace arcane \
      --set image.repository=ghcr.io/sneaksanddata/arcane-stream-sqlserver-change-tracking \
      --set image.tag=kind-dev \
      --set image.pullPolicy=IfNotPresent
  ```

#### Running streams in Kind 
To be added...

### Development
Project uses `Scala 3.8.3` and tested on JDK 25. 
