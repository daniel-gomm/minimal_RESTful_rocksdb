FROM openjdk:15-alpine

RUN apk add --no-cache libstdc++

COPY target/rocksdb_restful-0.0.1-SNAPSHOT.jar /RocksDB/application/rocksdb.jar

#ENV ROCKSDB_PATH="/etc/rocksdb"

WORKDIR /RocksDB/application

ENTRYPOINT [ "java", "-jar","rocksdb.jar" ]
