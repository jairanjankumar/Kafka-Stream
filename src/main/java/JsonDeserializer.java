import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Json Deserializer
 */
public class JsonDeserializer implements Deserializer<JsonNode> {
    private ObjectMapper objectMapper;

    public JsonDeserializer() {
        objectMapper = new ObjectMapper();
    }

    public void configure(Map<String, ?> map, boolean b) {
        //nothing to configure
    }

    /**
     * Deserialize to a JsonNode
     * @param topic topic name
     * @param data message bytes
     * @return JsonNode
     */
    public JsonNode deserialize(String topic, byte[] data) {
        if(data==null){
            return null;
        }
        try{
            return objectMapper.readTree(data);
        }catch (Exception e){
            throw new SerializationException(e);
        }
    }

    public void close() {
        //nothing to close
    }
}