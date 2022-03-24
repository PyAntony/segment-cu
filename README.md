# segment-cu Project

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```shell script
./mvnw compile quarkus:dev
```
Or if you have the Quarkus CLI installed:
```shell script
quarkus dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8080/q/dev/.

## Packaging and running the application

The application can be packaged using:
```shell script
./mvnw clean package
```
It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

Runtime variables can be passed using *-D* flags or env varibles. Examples:
```shell script
java -Dquarkus.http.port=8081 -jar target/quarkus-app/quarkus-run.jar
```
```shell script
export env QUARKUS_HTTP_PORT=8081; java -jar target/quarkus-app/quarkus-run.jar
```

### Deployment (for CI/CD)
The following env variables are required in deployment repo:

- quarkus.http.port=8080
- quarkus.http.host=0.0.0.0
- quarkus.kafka.health.enabled=false
- kafka.bootstrap.servers=localhost:9092

Variables should be uppercased and `.` replaced with `_`.

