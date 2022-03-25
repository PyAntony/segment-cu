FROM maven:3.6.3-openjdk-11 AS package

WORKDIR /build
COPY ./pom.xml /build/
COPY ./src/ /build/src

RUN pwd
RUN ls /build/src/main/
RUN ls /build/src/main/resources

# exclusion in .dockerignore: src/main/resources/*.properties
RUN mvn -Dmaven.test.skip=true \
    -Dquarkus.arc.exclude-types=com.charter.pauselive.scu.kafka.SampleProducer \
    # -Dquarkus.config.locations=file:/config \
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

# docker run -i --rm -p 8080:8080 <image-tag>
