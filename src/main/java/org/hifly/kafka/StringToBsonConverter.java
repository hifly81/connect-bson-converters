package org.hifly.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;

public class StringToBsonConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(StringToBsonConverter.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed for this simple converter
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new DataException("Not valid for source connectors!");
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {
            return Utility.byteToBson(value);
        } catch (Exception e) {
            throw new DataException(e.getMessage());
        }
    }


}

