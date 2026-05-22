build:
    mise exec -- sbt assembly

check:
    mise exec -- sbt scalafmtCheckAll

it: build
    #!/usr/bin/env bash
    set -euo pipefail

    # cleanup docker regardless of test outcome
    trap 'docker compose down' EXIT

    docker compose up -d --wait
    mise exec --env it -- sbt test

dev:
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f dev.env ]]; then
        echo "Missing .env.dev. Create it locally before running just dev." >&2
        exit 1
    fi

    just build
    mise exec --env dev -- java -Dlogback.configurationFile=src/main/resources/logback.xml -Dscala.concurrent.context.numThreads=2 -Dscala.concurrent.context.maxThreads=2 -Djava.net.preferIPv6Addresses=true -jar target/com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar
