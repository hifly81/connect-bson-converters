# Overview

Custom Kafka Connect Converters and SMTs applied to different binary inputs to prioduce a MongoDB Bson Document.

Converters:

 - org.hifly.kafka.OracleRawToBsonKeyConverter - byte [] to Oracle RAW to MongoDB Bson
 - org.hifly.kafka.ByteArrayAndStringConverter - pass through for byte arrays and strings

SMTs:

- org.hifly.kafka.smt.JsonKeyToValue - get value from _id and copy on value fields


## Install oracle jdbc driver in maven local repo

```bash
mvn install:install-file -Dfile=ojdbc10.jar -DgroupId=com.oracle -DartifactId=ojdbc10 -Dversion=19.3 -Dpackaging=jar
```

## Execute tests

```bash
mvn clean test
```

## Build and create distributable jar

```bash
mvn clean compile assembly:single
```

## Appendix

### MongoDB Sink Connector - properties

```json
{
  "name" : "mongo-sample-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "XXXXXXXX",
    "topics": "XXXXXXXX",
    "key.converter": "org.hifly.kafka.OracleRawToBsonKeyConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "database": "XXXXXXXX",
    "collection": "XXXXXXXX",
    "errors.tolerance": "all",
    "mongo.errors.log.enable": "true",
    "delete.on.null.values": "true",
    "document.id.strategy.overwrite.existing": "true",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
    "delete.writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy",
    "publish.full.document.only": "true",
    "transforms": "ReplaceField, addKeyToValue",
    "transforms.ReplaceField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.ReplaceField.exclude": "ID,XXXXXXXX,XXXXXXXX",
    "transforms.addKeyToValue.type": "org.hifly.kafka.smt.JsonKeyToValue",
    "transforms.addKeyToValue.valuename": "ID",
    "transforms.addKeyToValue.idkey": "_id"
  }
}

```