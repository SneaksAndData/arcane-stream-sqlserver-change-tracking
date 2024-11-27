FROM --platform=$BUILDPLATFORM sbtscala/scala-sbt:eclipse-temurin-23.0.1_11_1.10.5_3.5.2 AS build

WORKDIR /

COPY . .

WORKDIR /plugin/arcane-stream-sqlserver-change-tracking

RUN sbt assembly

FROM --platform=$BUILDPLATFORM gcr.io/distroless/java-base-debian12

COPY --from=build /plugin/arcane-stream-sqlserver-change-tracking/target/scala-3.6.1 /app

CMD ["java", "-jar", "/app/com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar"]
