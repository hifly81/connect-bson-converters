package org.hifly.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.hifly.kafka.smt.JsonKeyToValue;
import org.junit.Assert;
import org.junit.Test;


public class JsonKeyToValueTest {

    private final JsonKeyToValue<SinkRecord> xform = new JsonKeyToValue();


    @Test
    public void testValue() {

        final Map<String, Object> props = new HashMap<>();
        props.put("valuename", "ID");
        props.put("idkey", "_id");

        xform.configure(props);

        String keyValue = "{\"_id\": {\"_id\":\"sdsssss\"}}";
        String strValue = "{\"C_IST\": \"01\"}";

        final SinkRecord record = new SinkRecord("", 0, null, keyValue, null, strValue, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        Assert.assertNotNull(record.key());
        Assert.assertNotNull(record.value());

        System.out.println(transformedRecord.key());
        System.out.println(transformedRecord.value());

    }

    @Test
    public void testTombstone() {

        final Map<String, Object> props = new HashMap<>();
        props.put("valuename", "ID");
        props.put("idkey", "_id");

        xform.configure(props);

        String keyValue = "{\"_id\": {\"_id\":\"sdsssss\"}}";

        final SinkRecord record = new SinkRecord("", 0, null, keyValue, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        Assert.assertNotNull(record.key());
        Assert.assertNull(record.value());

        System.out.println(transformedRecord.key());
        System.out.println(transformedRecord.value());

    }

}