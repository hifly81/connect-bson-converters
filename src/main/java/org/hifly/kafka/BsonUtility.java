package org.hifly.kafka;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.connect.data.SchemaAndValue;
import oracle.sql.RAW;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.UUID;

public class BsonUtility {

    private static final String KEY = "_id";
    private static final Logger log = LoggerFactory.getLogger(BsonUtility.class);

    public static SchemaAndValue byteToBson(byte[] value) {

        String input;

        try {
            input = Base64.getEncoder().encodeToString(value);
        } catch (Exception ex) {
            log.error("Error in generating a Base64 value - SchemaAndValue.NULL will be returned", ex);
            return SchemaAndValue.NULL;
        }

        return generateBsonDocument(input);
    }

    public static SchemaAndValue oracleRawToBson(byte[] value) {

        String input;

        try {
            input = new RAW(value).stringValue();
        } catch (Exception ex) {
            log.error("Error in generating a RAW value - SchemaAndValue.NULL will be returned", ex);
            return SchemaAndValue.NULL;
        }

        return generateBsonDocument(input);
    }

    /*
     Oracle RAW byte [] size must be 16
     */
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

        finalValue += uuidString.substring(18, uuidString.length());

        try {
            return Hex.decodeHex(finalValue);
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }

    }

    private static SchemaAndValue generateBsonDocument(String input) {
        try {
            Document doc = new Document(KEY, input);
            String json = doc.toJson();

            log.info("Bson document for json {}", json);

            doc = Document.parse(json);

            return new SchemaAndValue(null, doc);
        } catch (Exception ex) {
            log.error("Error in generating a Bson document - SchemaAndValue.NULL will be returned", ex);
            return SchemaAndValue.NULL;
        }
    }
}
