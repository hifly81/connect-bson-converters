package org.hifly.kafka;

import org.apache.kafka.connect.data.SchemaAndValue;
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
        commonValidators(result);

    }

    @Test
    public void testOracleRawNullToBson () {

        byte [] b1 = new byte[RAW_BYTE_SIZE];
        SchemaAndValue result = BsonUtility.oracleRawToBson(b1);
        commonValidators(result);
    }

    private static String commonValidators(SchemaAndValue result) {
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.value());
        Assert.assertEquals(result.value().getClass().getName(), String.class.getName());
        String doc = (String)result.value();
        System.out.println(doc);
        return doc;
    }
}
