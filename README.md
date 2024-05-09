# Overview

Custom Kafka Connect Converters applied to different binary inputs.

 - org.hifly.kafka.ByteToBsonConverter - byte [] to base64 to MongoDB Bson
 - org.hifly.kafka.OracleRawToBsonConverter - byte [] to Oracle RAW to MongoDB Bson


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