# Overview

Custom Kafka Connect Converters applied to different binary inputs.

## Install jdbc driver in mavel local repo

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