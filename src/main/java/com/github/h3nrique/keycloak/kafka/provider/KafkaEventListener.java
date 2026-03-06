package com.github.h3nrique.keycloak.kafka.provider;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerTransaction;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.KeycloakSession;

/**
 * Classe que implementa o EventListenerProvider para escutar eventos do Keycloak e enviar para Kafka.
 * @author Paulo Henrique Alves
 * @version 1.0
 * @since 2026-03-03
 */
public class KafkaEventListener implements EventListenerProvider {

    private static final Logger logger = Logger.getLogger(KafkaEventListener.class);
    
    private final Properties kafkaConfig;
    private final String topicEvent;
    private final String topicAdminEvent;
    private final boolean kafkaDedicatedTopicEvents;
    private final boolean kafkaTransactionalEvents;
    private final boolean kafkaTransactionalAdminEvents;

    private final KeycloakSession keycloakSession;
    private final EventListenerTransaction eventListenerTransaction;

    /**
     * Construtor para KafkaEventListener.
     * @param keycloakSession
     * @param kafkaConfig
     */
    public KafkaEventListener(KeycloakSession keycloakSession, Properties kafkaConfig) {
        this.keycloakSession = keycloakSession;
        this.kafkaConfig = kafkaConfig;
        this.eventListenerTransaction = new EventListenerTransaction(this::processAdminEvent, this::processUserEvent);
        this.keycloakSession.getTransactionManager().enlistAfterCompletion(this.eventListenerTransaction);
        
        this.topicAdminEvent = System.getenv("KAFKA_TOPIC_NAME_ADMIN_EVENT") != null ? System.getenv("KAFKA_TOPIC_NAME_ADMIN_EVENT") : "keycloak.admin.event";
        this.topicEvent = System.getenv("KAFKA_TOPIC_NAME_EVENT") != null ? System.getenv("KAFKA_TOPIC_NAME_EVENT") : "keycloak.event";
        this.kafkaDedicatedTopicEvents = Boolean.parseBoolean(System.getenv("KAFKA_DEDICATED_TOPIC_EVENTS"));
        this.kafkaTransactionalEvents = Boolean.parseBoolean(System.getenv("KAFKA_TRANSACTIONAL_EVENTS"));
        this.kafkaTransactionalAdminEvents = Boolean.parseBoolean(System.getenv("KAFKA_TRANSACTIONAL_ADMIN_EVENTS"));

        logger.debugf("topicAdminEvent :: %s", this.topicAdminEvent);
        logger.debugf("topicEvent :: %s", this.topicEvent);
        logger.debugf("kafkaDedicatedTopicEvents :: %s", this.kafkaDedicatedTopicEvents);
        logger.debugf("kafkaTransactionalEvents :: %s", this.kafkaTransactionalEvents);
        logger.debugf("kafkaTransactionalAdminEvents :: %s", this.kafkaTransactionalAdminEvents);
        logger.debug("event listener initialized");
    }

    /**
     * Processa o evento de usuário.
     * @param event Evento recebido.
     */
    @Override
    public void onEvent(Event event) {
        logger.debugf("Event received :: %s", toString(event));
        if(kafkaTransactionalEvents) {
            eventListenerTransaction.addEvent(event);
        } else {
            processUserEvent(event);
        }
    }

    /**
     * Processa o evento de admin.
     * @param event Evento admin recebido.
     * @param includeRepresentation Se deve incluir o atributo 'representation'.
     */
    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        logger.debugf("AdminEvent received :: %s", toString(event, !includeRepresentation ? "representation" : null));
        if(kafkaTransactionalAdminEvents) {
            eventListenerTransaction.addAdminEvent(event, includeRepresentation);
        } else {
            processAdminEvent(event, includeRepresentation);
        }
    }

    /**
     * Process a user event and send it to Kafka.
     * @param event Evento do usuário.
     */
    private void processUserEvent(Event event) {
        EventType eventType = event.getType();
        String topic = kafkaDedicatedTopicEvents ? String.format("%s.%s", this.topicEvent, eventType) : this.topicEvent;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig)) {
            producer.send(new ProducerRecord<>(topic, event.getId(), toString(event)));
        }
    }

    /**
     * Process an admin event and send it to Kafka.
     * @param event O evento admin.
     * @param includeRepresentation Se deve incluir o atributo 'representation'.
     */
    private void processAdminEvent(AdminEvent event, Boolean includeRepresentation) {
        ResourceType resourceType = event.getResourceType();
        String topic = kafkaDedicatedTopicEvents ? String.format("%s.%s", this.topicAdminEvent, resourceType) : this.topicAdminEvent;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig)) {
            producer.send(new ProducerRecord<>(topic, event.getId(), toString(event, !includeRepresentation ? "representation" : null)));
        }
    }

    /**
     * Converte um objeto para uma string JSON.
     * @param object O objeto a ser convertido.
     * @param ignoredFields Campos a serem ignorados.
     * @return A string JSON.
     */
    public static <T> String toString(T object, String... ignoredFields) {
        Class<?> clazz = object.getClass();
        StringBuilder objectValues = new StringBuilder(clazz.getSimpleName()).append(" { ");
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.getName().equals("serialVersionUID") || Arrays.asList(ignoredFields).contains(field.getName())) continue;
            Object value = getFieldValue(object, field);
            objectValues.append(field.getName()).append(":").append(value).append(", ");
        }
        objectValues.append('}');
        return objectValues.toString();
    }

    /**
     * Obtém o valor de um campo de um objeto e converte para string.
     * @param <T> O tipo do objeto.
     * @param object O objeto.
     * @param field O campo.
     * @return O valor do campo convertido para string.
     */
    @SuppressWarnings("UseSpecificCatch")
    private static <T> String getFieldValue(T object, Field field) {
        try {
            field.setAccessible(true);
            Object value = field.get(object);
            if (value == null) {
                return "null";
            } else if (value instanceof String string) {
                return '"' + string + '"';
            } else if (value.getClass().isArray()) {
                return Arrays.toString((Object[]) value);
            }
            return value.toString();
        } catch (Exception err) {
            return err.toString();
        }
    }

    /**
     * Fecha os objetos construidos.
     */
    @Override
    public void close() {
        // Empty
    }

}
