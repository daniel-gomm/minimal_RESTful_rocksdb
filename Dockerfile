FROM openjdk:15-alpine

RUN apk add --no-cache libstdc++

COPY rocksdb_restful-0.0.1.jar /RocksDB/application/rocksdb.jar

#ENV ROCKSDB_PATH="/etc/rocksdb"

WORKDIR /RocksDB/application

ENTRYPOINT [ "java", "-jar","rocksdb.jar" ]