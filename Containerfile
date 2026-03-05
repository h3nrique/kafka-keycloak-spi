FROM docker.io/library/maven:3-eclipse-temurin-21-alpine AS builder
COPY . .
RUN mvn clean package -DskipTests

FROM registry.redhat.io/rhbk/keycloak-rhel9:26.4-11 as keycloak
COPY --from=builder --chown=keycloak:keycloak --chmod=644 target/kafka-keycloak-spi-jar-with-dependencies.jar /opt/keycloak/providers/kafka-keycloak-spi.jar
ENV KC_HEALTH_ENABLED=true KC_METRICS_ENABLED=true
RUN /opt/keycloak/bin/kc.sh build
