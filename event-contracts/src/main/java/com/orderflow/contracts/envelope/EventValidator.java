package com.orderflow.contracts.envelope;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Validates Event Envelopes and Payloads against JSON Schema v1 definitions.
 */
public class EventValidator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final JsonSchemaFactory SCHEMA_FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    private static final Map<String, JsonSchema> SCHEMA_CACHE = new ConcurrentHashMap<>();

    private static final String ENVELOPE_SCHEMA_PATH = "/schemas/envelope-v1.json";
    private static final Map<String, String> EVENT_SCHEMA_MAP = Map.of(
            "order.placed", "/schemas/order-placed-v1.json",
            "inventory.reserved", "/schemas/inventory-reserved-v1.json",
            "inventory.rejected", "/schemas/inventory-rejected-v1.json",
            "order.confirmed", "/schemas/order-confirmed-v1.json",
            "order.failed", "/schemas/order-failed-v1.json",
            "order.cancel.requested", "/schemas/order-cancel-requested-v1.json",
            "inventory.released", "/schemas/inventory-released-v1.json",
            "order.cancelled", "/schemas/order-cancelled-v1.json"
    );

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * Validates raw JSON string against envelope schema and specific event payload schema.
     */
    public static void validateJson(String json) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(json);
            validateJsonNode(rootNode);
        } catch (JsonProcessingException e) {
            throw new EventValidationException("Malformed JSON payload: " + e.getMessage(), e);
        }
    }

    /**
     * Validates parsed JsonNode against envelope schema and specific event payload schema.
     */
    public static void validateJsonNode(JsonNode rootNode) {
        // 1. Validate envelope
        JsonSchema envelopeSchema = loadSchema(ENVELOPE_SCHEMA_PATH);
        Set<ValidationMessage> envelopeErrors = envelopeSchema.validate(rootNode);
        if (!envelopeErrors.isEmpty()) {
            String errorDetails = envelopeErrors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("; "));
            throw new EventValidationException("Envelope validation failed: " + errorDetails);
        }

        // 2. Validate event type schema
        String eventType = rootNode.path("eventType").asText();
        String schemaPath = EVENT_SCHEMA_MAP.get(eventType);
        if (schemaPath == null) {
            throw new EventValidationException("Unsupported eventType: " + eventType);
        }

        JsonNode dataNode = rootNode.path("data");
        JsonSchema dataSchema = loadSchema(schemaPath);
        Set<ValidationMessage> dataErrors = dataSchema.validate(dataNode);
        if (!dataErrors.isEmpty()) {
            String errorDetails = dataErrors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("; "));
            throw new EventValidationException("Payload validation failed for '" + eventType + "': " + errorDetails);
        }
    }

    /**
     * Serializes EventEnvelope to validated JSON string.
     */
    public static <T> String serializeAndValidate(EventEnvelope<T> envelope) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(envelope);
            validateJson(json);
            return json;
        } catch (JsonProcessingException e) {
            throw new EventValidationException("Failed to serialize envelope: " + e.getMessage(), e);
        }
    }

    /**
     * Deserializes and validates raw JSON to EventEnvelope with typed data.
     */
    public static <T> EventEnvelope<T> deserializeAndValidate(String json, Class<T> dataType) {
        validateJson(json);
        try {
            var javaType = OBJECT_MAPPER.getTypeFactory().constructParametricType(EventEnvelope.class, dataType);
            return OBJECT_MAPPER.readValue(json, javaType);
        } catch (JsonProcessingException e) {
            throw new EventValidationException("Failed to deserialize envelope: " + e.getMessage(), e);
        }
    }

    private static JsonSchema loadSchema(String resourcePath) {
        return SCHEMA_CACHE.computeIfAbsent(resourcePath, path -> {
            InputStream is = EventValidator.class.getResourceAsStream(path);
            if (is == null) {
                throw new IllegalStateException("Schema resource not found: " + path);
            }
            return SCHEMA_FACTORY.getSchema(is);
        });
    }
}
