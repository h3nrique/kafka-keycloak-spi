package com.github.h3nrique.keycloak.kafka.provider;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import com.google.auto.service.AutoService;

/**
 * Classe que implementa o EventListenerProviderFactory para criar o KafkaEventListener.
 * @author Paulo Henrique Alves
 * @version 1.0
 * @since 2026-03-03
 */
@AutoService(EventListenerProviderFactory.class)
public class KafkaEventProviderFactory implements EventListenerProviderFactory {

    private static final Logger logger = Logger.getLogger(KafkaEventProviderFactory.class);
    public final static String ID = "kafka-event-listener";

    /**
     * Obtém o ID do provedor.
     * @return O ID do provedor.
     */
    @Override
    public String getId() {
        return ID;
    }

    /**
     * Cria o KafkaEventListener.
     * @param keycloakSession A sessão do Keycloak.
     * @return O KafkaEventListener.
     */
    @Override
    public EventListenerProvider create(KeycloakSession keycloakSession) {
        logger.trace("create");

        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "kafka:9092";
        String producerAcks = System.getenv("KAFKA_ACKS") != null ? System.getenv("KAFKA_ACKS") : "0";
        String kafkaRetriesConfig = System.getenv("KAFKA_RETRIES_CONFIG") != null ? System.getenv("KAFKA_RETRIES_CONFIG") : "3";
        String hostname = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "null";

        logger.debugf("hostname :: %s", hostname);
        logger.debugf("kafkaBootstrapServers :: %s", kafkaBootstrapServers);
        logger.debugf("producerAcks :: %s", producerAcks);
        logger.debugf("kafkaRetriesConfig :: %s", kafkaRetriesConfig);

        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        kafkaConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, String.format("client-%s", hostname));
        kafkaConfig.setProperty(ProducerConfig.ACKS_CONFIG, producerAcks);
        kafkaConfig.setProperty(ProducerConfig.RETRIES_CONFIG, kafkaRetriesConfig);

        return new KafkaEventListener(keycloakSession, kafkaConfig);
    }

    /**
     * Inicializa o provedor.
     * @param keycloakSessionFactory A fábrica de sessões do Keycloak.
     */
    @Override
    public void postInit(KeycloakSessionFactory keycloakSessionFactory) {
        logger.trace("postInit");
    }

    /**
     * Fecha o provedor.
     */
    @Override
    public void close() {
        logger.trace("close");
    }

    /**
     * Inicializa o provedor.
     * @param config A configuração do provedor.
     */
    @Override
    public void init(Config.Scope config) {
        logger.trace("init");
    }

}
