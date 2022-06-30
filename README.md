# SCU (Segment Catch-Up) Project

Application republishes already downloaded segments on the same topic. There is some context 
that needs to be understood first: 
- When `Segment Downloader` uploads a new segment (in `segment-ready` topic), it also sends a 
message with metadata (to `segment-ready-keys`) about this message; metadata includes the partition 
and offset where the SegmentReady message was published.
- `MSPlayer-Q` keeps track of the current segment number of the live manifest. When a request from 
`PaLM` is received with an older segment (e.g., current segment is 1000 and `PaLM` sends a request 
with 990) `MSPlayer-Q` sends a message with that segment range (990 to 1000) to the `segment-copy-from-ready` 
topic. This is done since `MSPlayer-Q` only downloads live manifests 
and sends notifications to `Segment Downloader` for live segments. It is assumed that 
older segment numbers have been already downloaded and stored somewhere.

`SCU` keeps an internal cache of messages consumed from `segment-ready-keys`; i.e., it knows where to find older segments. It 
also consumes messages from `segment-copy-from-ready`, so it knows which segments need to be searched and republished. 
Using its internal cache `SCU` searches for the position (partition, offset) of this older segments in the `segment-ready` 
topic, fetches the full message and republishes it again in the same topic. `Copy Service` now can consume older 
segments.

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

