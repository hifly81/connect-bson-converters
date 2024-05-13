package org.hifly.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class JsonKeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String ROWKEY_CONFIG = "valuename";

    private static final String ID_KEY = "_id";


    public static final String OVERVIEW_DOC = "Add the record key to the value as a field.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ROWKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name to add.");

    private static final String PURPOSE = "Add the record key to the value as a field.";

    private static final Logger log = LoggerFactory.getLogger(JsonKeyToValue.class);

    private String rowKey;

    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        rowKey = config.getString(ROWKEY_CONFIG);
    }

    public R apply(R record) {
        return applySchemaless(record);
    }

    private R applySchemaless(R record) {

        Map<String, Object> value;

        // Tombstone message handling
        if(record.value() == null) {
            log.info("Tombstone record found for key {}", record.key());
            // Generate a empty value
            value = new HashMap<>();
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    record.key(),
                    record.valueSchema(),
                    value,
                    record.timestamp()
            );
        }

        try {
            value = (Map<String, Object>) record.value();
        } catch (Exception e) {
            log.error("Can't parse record.value", e);
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    record.key(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }

        try {
            // Get the value from a json key
            JSONObject obj = new JSONObject(record.key().toString());
            JSONObject jsonObject = new JSONObject(obj.toString());
            //Copy key value in value
            String innerValue = jsonObject.getString(ID_KEY);
            value.put(rowKey, innerValue);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    record.key(),
                    record.valueSchema(),
                    value,
                    record.timestamp()
            );
        } catch (Exception e) {
            log.error("Can't parse " + ID_KEY, e);
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    record.key(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }
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