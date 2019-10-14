import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Json Serializer
 */
public class JsonSerializer implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonSerializer() {

    }

    public void configure(Map<String, ?> config, boolean isKey) {
        //Nothing to Configure
    }

    /**
     * Serialize JsonNode
     *
     * @param topic Kafka topic name
     * @param data  data as JsonNode
     * @return byte[]
     */
    public byte[] serialize(String topic, JsonNode data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    public void close() {

    }
}