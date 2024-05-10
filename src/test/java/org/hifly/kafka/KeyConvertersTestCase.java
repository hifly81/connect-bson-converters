package org.hifly.kafka;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

public class KeyConvertersTestCase {

    private static final String KEY = "_id";
    private static final String RAW_DEFAULT_VALUE = "00000000000000000000000000000000";
    private static final int RAW_BYTE_SIZE = 16;

    @Test
    public void testByteToBson () {

        byte [] b1 = new byte[RAW_BYTE_SIZE];
        new Random().nextBytes(b1);
        SchemaAndValue result = BsonUtility.byteToBson(b1);
        commonValidators(result);
    }

    @Test
    public void testOracleRawToBson () {

        byte [] b1 = BsonUtility.convertToOracleRaw(UUID.randomUUID());
        Assert.assertEquals(b1.length, RAW_BYTE_SIZE);
        SchemaAndValue result = BsonUtility.oracleRawToBson(b1);
        Document doc = commonValidators(result);
        Assert.assertNotEquals(RAW_DEFAULT_VALUE, doc.get(KEY));

    }

    @Test
    public void testOracleRawNullToBson () {

        byte [] b1 = new byte[RAW_BYTE_SIZE];
        SchemaAndValue result = BsonUtility.oracleRawToBson(b1);
        Document doc = commonValidators(result);
        Assert.assertEquals(RAW_DEFAULT_VALUE, doc.get(KEY));

    }

    private static Document commonValidators(SchemaAndValue result) {
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.value());
        Assert.assertEquals(result.value().getClass().getName(), org.bson.Document.class.getName());
        Document doc = (Document)result.value();
        Assert.assertTrue(doc.containsKey(KEY));
        Assert.assertNotNull(doc.get(KEY));
        System.out.println(result);
        return doc;
    }
}
