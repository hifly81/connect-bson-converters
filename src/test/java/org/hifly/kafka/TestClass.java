package it.gse.kafka;

import org.junit.Test;

import java.util.Random;
import java.util.UUID;

public class TestClass {

    @Test
    public void testByteToString () {
        byte [] b1 = new byte[20];
        new Random().nextBytes(b1);
        System.out.println(Utility.byteToString(b1));

        byte [] b2 = new byte[20];
        new Random().nextBytes(b2);
        System.out.println(Utility.byteToString(b2));
    }

    @Test
    public void testByteToBson () {
        byte [] b1 = new byte[20];
        new Random().nextBytes(b1);
        System.out.println(Utility.byteToBson(b1));

        byte [] b2 = new byte[20];
        new Random().nextBytes(b2);
        System.out.println(Utility.byteToBson(b2));
    }

    @Test
    public void testByteToRawAndBson () {
        byte [] b1 = Utility.convertToOracleRaw(UUID.randomUUID());
        System.out.println(Utility.byteToRawAndBson(b1));

        byte [] b2 = Utility.convertToOracleRaw(UUID.randomUUID());
        System.out.println(Utility.byteToRawAndBson(b2));
    }
}
