package com.shopsphere.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSON -> ShopEvent. Malformed records return null and are skipped (never crash the job). */
public class ShopEventDeserializer extends AbstractDeserializationSchema<ShopEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ShopEventDeserializer.class);
    private transient ObjectMapper mapper;

    @Override
    public ShopEvent deserialize(byte[] message) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            JsonNode n = mapper.readTree(message);
            String type = n.path("type").asText("");
            if (type.isEmpty()) {
                return null;
            }
            return new ShopEvent(
                    type,
                    n.path("user_id").asText("anonymous"),
                    n.path("amount").asDouble(0.0),
                    n.path("ts").asLong(0L));
        } catch (Exception e) {
            LOG.warn("Skipping malformed event: {}", e.getMessage());
            return null;
        }
    }
}
