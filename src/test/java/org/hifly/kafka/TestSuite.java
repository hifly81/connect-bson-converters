package org.hifly.kafka;

import org.junit.Test;

import java.util.Random;
import java.util.UUID;

public class TestSuite {

    @Test
    public void testByteToJsonString () {

        System.out.println("--> testByteToJsonString");

        byte [] b1 = new byte[20];
        new Random().nextBytes(b1);
        System.out.println(Utility.byteToJsonString(b1));

        byte [] b2 = new byte[20];
        new Random().nextBytes(b2);
        System.out.println(Utility.byteToJsonString(b2));
    }

    @Test
    public void testByteToBson () {

        System.out.println("--> testByteToBson");

        byte [] b1 = new byte[20];
        new Random().nextBytes(b1);
        System.out.println(Utility.byteToBson(b1));

        byte [] b2 = new byte[20];
        new Random().nextBytes(b2);
        System.out.println(Utility.byteToBson(b2));
    }

    @Test
    public void testOracleRawToBson () {

        System.out.println("--> testOracleRawToBson");

        byte [] b1 = Utility.convertToOracleRaw(UUID.randomUUID());
        System.out.println(Utility.oracleRawToBson(b1));

        byte [] b2 = Utility.convertToOracleRaw(UUID.randomUUID());
        System.out.println(Utility.oracleRawToBson(b2));
    }
}
