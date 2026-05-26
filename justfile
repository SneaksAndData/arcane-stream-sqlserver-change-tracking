build:
    mise exec -- sbt assembly

clean:
    mise exec -- sbt clean

check:
    mise exec -- sbt scalafmtCheckAll

it: build
    #!/usr/bin/env bash
    set -euo pipefail

    # cleanup docker regardless of test outcome
    trap 'docker compose down' EXIT

    docker compose up -d
    docker compose wait prepare_buckets lakekeeper_migrate lakekeeper_prepare setup_mssql
    mise exec --env it -- sbt test

stream:
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f dev.env ]]; then
        echo "Missing dev.env, create it locally before running this command." >&2
        exit 1
    fi

    just build
    mise exec --env dev -- env STREAMCONTEXT__BACKFILL=${STREAMCONTEXT__BACKFILL:-false} java -Dlogback.configurationFile=src/main/resources/logback.xml -Dscala.concurrent.context.numThreads=2 -Dscala.concurrent.context.maxThreads=2 -Djava.net.preferIPv6Addresses=true -jar target/com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar

backfill:
    STREAMCONTEXT__BACKFILL=true just stream
