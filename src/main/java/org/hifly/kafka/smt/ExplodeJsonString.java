package org.hifly.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ExplodeJsonString<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String ROWKEY_CONFIG = "valuename";
    public static final String OVERVIEW_DOC = "Create a struct for a JSON Field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ROWKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name to add.");

    private static final String PURPOSE = "Add the record key to the value as a field.";

    private static final Logger log = LoggerFactory.getLogger(ExplodeJsonString.class);

    private String rowKey;

    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        rowKey = config.getString(ROWKEY_CONFIG);
    }

    public R apply(R record) {
        return applySchemaless(record);
    }

    private R applySchemaless(R record) {

        // Get the value of the record (it should be a Struct in this case)
        if (record.value() == null) {
            return record;
        }

        Struct value = (Struct) record.value();
        Schema originalSchema = value.schema();

        // Get the json_data field from the record
        String jsonData = value.getString(rowKey);
        if (jsonData == null) {
            return record;  // Return the record unchanged if json_data is null
        }


        // Parse json_data into a Map
        Map<String, Object> parsedJson;
        try {
            parsedJson = objectMapper.readValue(jsonData, Map.class);
        } catch (Exception e) {
            throw new ConnectException("Error parsing json_data field", e);
        }

        SchemaBuilder newSchemaBuilder = SchemaBuilder.struct();
        for (Field field : originalSchema.fields()) {
            if(!field.name().equals(rowKey))
                newSchemaBuilder.field(field.name(), field.schema());
        }
        for (String key : parsedJson.keySet()) {
            newSchemaBuilder.field(key, Schema.OPTIONAL_STRING_SCHEMA); // Adjust the schema type as needed
        }
        Schema newSchema = newSchemaBuilder.build();

        // Create a new Struct to include the original fields and flattened json fields
        Struct newValue = new Struct(newSchema);

        // Add original fields expect json data
        for (Field field : newValue.schema().fields()) {
            String fieldName = field.name();
            if(!fieldName.equals(rowKey)) {
                try {
                    Object fieldValue = value.get(fieldName);
                    newValue.put(fieldName, fieldValue);
                } catch(Exception ex) {

                }

            }
        }

        // Flatten the json_data and add its fields
        for (Map.Entry<String, Object> entry : parsedJson.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            // Handle nested objects
            if (fieldValue instanceof Map) {
                // Convert nested objects to structs
                // TODO
                newValue.put(fieldName,fieldValue.toString());
            } else {
                newValue.put(fieldName, fieldValue.toString());
            }
        }

        // Return the transformed record with the new value
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newValue.schema(),
                newValue,
                record.timestamp());

    }

    private R applyWithSchema(R record) {
        return applySchemaless(record);
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {

    }

}