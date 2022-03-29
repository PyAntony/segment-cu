FROM maven:3.6.3-openjdk-11 AS package

WORKDIR /build
COPY ./pom.xml /build/
COPY ./src/ /build/src

RUN mvn -Dmaven.test.skip=true \
    -Dquarkus.arc.exclude-types=com.charter.pauselive.scu.kafka.sample.SampleProducer \
    clean package

FROM registry.access.redhat.com/ubi8/openjdk-11:1.11

ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en'

# We make four distinct layers so if there are application changes the library layers can be re-used
COPY --chown=185 --from=package /build/target/quarkus-app/lib/ /deployments/lib/
COPY --chown=185 --from=package /build/target/quarkus-app/*.jar /deployments/
COPY --chown=185 --from=package /build/target/quarkus-app/app/ /deployments/app/
COPY --chown=185 --from=package /build/target/quarkus-app/quarkus/ /deployments/quarkus/

EXPOSE 8080
USER 185
ENV AB_JOLOKIA_OFF=""
ENV JAVA_OPTS="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"

# src/application.properties is excluded from jar file (defined in pom.xml)
ENV QUARKUS_CONFIG_LOCATIONS=file:/config,file:/home/jboss/config

# docker run -i --rm -p 8080:8080 <image-tag>