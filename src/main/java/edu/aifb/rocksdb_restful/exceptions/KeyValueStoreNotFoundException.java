package edu.aifb.rocksdb_restful.exceptions;


import java.nio.charset.StandardCharsets;

public class KeyValueStoreNotFoundException extends IllegalArgumentException{

    public KeyValueStoreNotFoundException(byte[] column){
        super("Could not find KeyValueStore " + new String(column, StandardCharsets.UTF_8));
    }
}
