package org.hifly.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.connect.data.SchemaAndValue;
import oracle.sql.RAW;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.UUID;

public class Utility {

    private static final Logger log = LoggerFactory.getLogger(Utility.class);

    public static SchemaAndValue byteToBson(byte[] value) {

        String input = Base64.getEncoder().encodeToString(value);

        Document doc = new Document("_id", input);
        String jsonString = doc.toJson();

        log.info("json string {}", jsonString);

        doc = Document.parse(jsonString);

        return new SchemaAndValue(null, doc);
    }

    public static SchemaAndValue byteToRawAndBson(byte[] value) {

        String input = new RAW(value).stringValue();

        Document doc = new Document("_id", input);
        String jsonString = doc.toJson();

        log.info("json string {}", jsonString);

        doc = Document.parse(jsonString);

        return new SchemaAndValue(null, doc);
    }

    public static SchemaAndValue byteToString(byte[] value) {
        String input = Base64.getEncoder().encodeToString(value);

        // Create a JSON object
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_id", input);

        Gson gson = new Gson();
        String jsonString = gson.toJson(jsonObject);

        log.info("json string {}", jsonString);

        return new SchemaAndValue(null, jsonString);
    }

    public static byte[] convertToOracleRaw(UUID uuid) {
        String uuidString = uuid.toString().replace("-", "").toUpperCase();
        String finalValue = "";
        finalValue += uuidString.substring(6,8);
        finalValue += uuidString.substring(4,6);
        finalValue += uuidString.substring(2,4);
        finalValue += uuidString.substring(0,2);
        finalValue += uuidString.substring(10,12);
        finalValue += uuidString.substring(8,10);
        finalValue += uuidString.substring(14,16);
        finalValue += uuidString.substring(12,14);
        finalValue += uuidString.substring(16,18);

        finalValue += uuidString.substring(18, uuidString.length());  // final string

        try {
            return Hex.decodeHex(finalValue);
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }

    }
}
