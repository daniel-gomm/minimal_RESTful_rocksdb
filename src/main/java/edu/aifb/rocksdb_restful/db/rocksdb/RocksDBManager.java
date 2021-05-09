package edu.aifb.rocksdb_restful.db.rocksdb;

import edu.aifb.rocksdb_restful.exceptions.KeyValueStoreNotFoundException;
import edu.aifb.rocksdb_restful.exceptions.advice.KeyValueStoreNotFoundAdvice;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class RocksDBManager {

    private final Logger LOG = LoggerFactory.getLogger(RocksDBManager.class);
    private static RocksDBManager instance;
    private final HashMap<String, RocksDBKeyValueStore> keyValueStores;
    private final RocksDBConnector connector;

    public static RocksDBManager getInstance(){
        if(instance == null){
            instance = new RocksDBManager();
        }
        return instance;
    }

    private RocksDBManager(){
        String path = (System.getenv().get("ROCKSDB_PATH") == null)?"/etc/rocksdb":System.getenv().get(
                "ROCKSDB_PATH");
        this.connector = RocksDBConnector.getInstance(path);
        this.keyValueStores = new HashMap<>();
        this.connector.getColumnFamilyHandles().forEach(cfh -> {
            try {
                this.keyValueStores.put(new String(cfh.getName(), StandardCharsets.UTF_8), new RocksDBKeyValueStore(cfh));
                LOG.info(String.format("Column family %s has been registered.", new String(cfh.getName(),
                        StandardCharsets.UTF_8)));
            } catch (RocksDBException | IllegalAccessException e) {
                LOG.error("Could not initialize column family.", e);
            }
        });
    }

    public RocksDBKeyValueStore getKeyValueStore(byte[] column) throws IllegalAccessException {
        if(keyValueStores.containsKey(new String(column)))
            return this.keyValueStores.get(new String(column));
        else throw new KeyValueStoreNotFoundException(column);
    }

    public void addKeyValueStore(byte[] column){
        if (this.keyValueStores.containsKey(new String(column))) return;
        ColumnFamilyHandle cfh = connector.registerColumnFamily(column);
        RocksDBKeyValueStore keyValueStore;
        try {
            keyValueStore = new RocksDBKeyValueStore(cfh);
            this.keyValueStores.put(new String(column), keyValueStore);
        } catch (IllegalAccessException e) {
            LOG.error("Could not create the KeyValueStore.", e);
        }
    }

    public boolean removeKeyValueStore(byte[] column){
        if (!keyValueStores.containsKey(new String(column))){
            LOG.info(String.format("Could not remove KeyValueStore %s. There is no such KeyValueStore.",
                    new String(column, StandardCharsets.UTF_8)));
            return false;
        }
        try {
            connector.deleteColumnFamily(this.keyValueStores.get(new String(column)).getColumnFamilyHandle());
        } catch (RocksDBException e) {
            LOG.info(String.format("Could not remove KeyValueStore %s. An exception occurred.",
                    new String(column, StandardCharsets.UTF_8)), e);
            return false;
        }
        LOG.info(String.format("Removed KeyValueStore %s.", new String(column, StandardCharsets.UTF_8)));
        return true;
    }

}
