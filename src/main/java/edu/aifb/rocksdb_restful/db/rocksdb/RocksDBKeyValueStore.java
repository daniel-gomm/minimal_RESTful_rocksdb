package edu.aifb.rocksdb_restful.db.rocksdb;

import edu.aifb.rocksdb_restful.db.model.KeyValueStore;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RocksDBKeyValueStore implements KeyValueStore<byte[], byte[]> {

    private final ColumnFamilyHandle columnFamilyHandle;
    private final RocksDBConnector connector;
    private final Logger LOG = LoggerFactory.getLogger(RocksDBKeyValueStore.class);

    public RocksDBKeyValueStore(ColumnFamilyHandle columnFamilyHandle) throws IllegalAccessException {
        this.columnFamilyHandle = columnFamilyHandle;
        this.connector = RocksDBConnector.getInstance();
    }

    public ColumnFamilyHandle getColumnFamilyHandle(){
        return this.columnFamilyHandle;
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        try {
            connector.put(key, value, this.columnFamilyHandle);
            return true;
        } catch (RocksDBException e) {
            LOG.error("Could not save " + new String(key, StandardCharsets.UTF_8));
            return false;
        }
    }

    @Override
    public boolean update(byte[] key, byte[] value) {
        return (delete(key) && put(key, value)) ;
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return  connector.get(key, this.columnFamilyHandle);
        } catch (RocksDBException e) {
            LOG.error("Could not find key " + new String(key), e);
            return null;
        }
    }

    @Override
    public boolean delete(byte[] key) {
        try {
            connector.delete(key, columnFamilyHandle);
            return true;
        } catch (RocksDBException e) {
            LOG.error("Could not delete element " + new String(key), e);
            return false;
        }
    }
}
