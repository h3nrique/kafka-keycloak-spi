package com.github.h3nrique.keycloak.kafka.provider;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jboss.logging.Logger;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.Assert;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaEventProviderTest {

    private static final Logger logger = Logger.getLogger(KafkaEventProviderTest.class);
    
    private static final String REALM = "example";
    private static final String REALM_URI = "/realms/".concat(REALM);
    private static final Network testNetwork = Network.newNetwork();
    private static final String EVENT_TOPIC = "keycloak.event";
    
    @SuppressWarnings("resource")
    @Container
    public static KeycloakContainer keycloak = new KeycloakContainer("registry.redhat.io/rhbk/keycloak-rhel9:26.4-12")
        .withRealmImportFile("example-realm.json")
        .withDefaultProviderClasses()
        .withAdminUsername("admin")
        .withAdminPassword("admin")
        .withNetwork(testNetwork)
        .withNetworkAliases("keycloak")
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9095")
        .withEnv("THROW_EXCEPTION_IF_ERROR_SENDING_EVENTS", "true")
        .withEnv("KAFKA_MAX_BLOCK_MS", "3000")
        .withEnv("KAFKA_ACKS", "all")
        .withProviderLibsFrom(Maven.resolver()
            .loadPomFromFile("./pom.xml")
            .resolve("org.apache.kafka:kafka-clients")
            .withTransitivity()
            .asList(File.class))
        .waitingFor(Wait.forListeningPorts(8080));
    
    @SuppressWarnings("resource")
    @Container
    public static KeycloakContainer keycloakWithoutKafka = new KeycloakContainer("registry.redhat.io/rhbk/keycloak-rhel9:26.4-12")
        .withRealmImportFile("example-realm.json")
        .withDefaultProviderClasses()
        .withAdminUsername("admin")
        .withAdminPassword("admin")
        .withNetwork(testNetwork)
        .withNetworkAliases("keycloak-without-kafka")
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9090") // Invalid kafka port
        .withEnv("THROW_EXCEPTION_IF_ERROR_SENDING_EVENTS", "true")
        .withEnv("KAFKA_MAX_BLOCK_MS", "3000")
        .withEnv("KAFKA_ACKS", "all")
        .withProviderLibsFrom(Maven.resolver()
            .loadPomFromFile("./pom.xml")
            .resolve("org.apache.kafka:kafka-clients")
            .withTransitivity()
            .asList(File.class))
        .waitingFor(Wait.forListeningPorts(8080));

    @SuppressWarnings("resource")
    @Container
    public static KafkaContainer kafka = new KafkaContainer("apache/kafka")
        .withNetworkAliases("kafka")
        .withNetwork(testNetwork)
        .withListener("kafka:9095")
        .withEnv("KAFKA_NUM_PARTITIONS", "3")
        .waitingFor(Wait.forListeningPorts(9092));

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
        final String accessToken = response
                .then()
                .assertThat()
                .statusCode(200)
                .extract()
                .path("access_token");
        logger.infof("Access token :: %s", accessToken);
        Assert.assertNotNull(accessToken);
    }

    @Order(2)
    @Test
    public void loadKafkaMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(EVENT_TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
            Assert.assertFalse("Should have received at least one message", records.isEmpty());
            records.forEach(record -> logger.infof("Message :: %s", record.value()));
        }
    }

    @Order(3)
    @Test
    public void testLoginError() {
        final String authServerUrl = getAuthServerUrlWithoutKafka();
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
        logger.info("Login error as expected");
    }

    private String getAuthServerUrl() {
        Assert.assertTrue(keycloak.isRunning());
        final String authServerUrl = keycloak.getAuthServerUrl();
        logger.info("Auth server url: " + authServerUrl);
        return authServerUrl;
    }

    private String getAuthServerUrlWithoutKafka() {
        Assert.assertTrue(keycloak.isRunning());
        final String authServerUrl = keycloakWithoutKafka.getAuthServerUrl();
        logger.info("Auth server url without kafka: " + authServerUrl);
        return authServerUrl;
    }

}
