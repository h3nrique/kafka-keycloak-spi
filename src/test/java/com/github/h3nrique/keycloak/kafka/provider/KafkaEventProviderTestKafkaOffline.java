package com.github.h3nrique.keycloak.kafka.provider;

import java.io.File;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.Assert;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaEventProviderTestKafkaOffline {

    private static final Logger logger = Logger.getLogger(KafkaEventProviderTestKafkaOffline.class);
    
    private static final String REALM = "example";
    private static final String REALM_URI = "/realms/".concat(REALM);
    
    @SuppressWarnings("resource")
    @Container
    public static KeycloakContainer keycloak = new KeycloakContainer("registry.redhat.io/rhbk/keycloak-rhel9:26.4-12")
        .withRealmImportFile("example-realm.json")
        .withDefaultProviderClasses()
        .withAdminUsername("admin")
        .withAdminPassword("admin")
        .withEnv("THROW_EXCEPTION_IF_ERROR_SENDING_EVENTS", "true")
        .withEnv("KAFKA_MAX_BLOCK_MS_CONFIG", "3000")
        .withEnv("KAFKA_ACKS", "all")
        .withProviderLibsFrom(Maven.resolver()
            .loadPomFromFile("./pom.xml")
            .resolve("org.apache.kafka:kafka-clients")
            .withTransitivity()
            .asList(File.class))
        .waitingFor(Wait.forListeningPorts(8080));

    @Order(1)
    @Test
    public void testLogin() {
        final String authServerUrl = getAuthServerUrl();
        Response response = RestAssured.given().contentType(ContentType.URLENC)
                .formParams(Map.of(
                    "username", "otto",
                    "password", "otto",
                    "grant_type", "password",
                    "client_id", "example"))
                .post(authServerUrl + REALM_URI + "/protocol/openid-connect/token");
        response
                .then()
                .assertThat()
                .statusCode(500);
        Assert.assertTrue("Should have received a 500 status code", true);
    }

    private String getAuthServerUrl() {
        Assert.assertTrue(keycloak.isRunning());
        final String authServerUrl = keycloak.getAuthServerUrl();
        logger.info("Auth server url: " + authServerUrl);
        return authServerUrl;
    }

}
