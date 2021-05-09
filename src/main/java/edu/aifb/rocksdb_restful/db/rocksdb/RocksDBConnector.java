package edu.aifb.rocksdb_restful.db.rocksdb;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class RocksDBConnector {

    private static RocksDBConnector instance;
    private final Logger LOG = LoggerFactory.getLogger(RocksDBConnector.class);
    private final String path;
    private final List<RocksObject> rocksObjects;
    private final Set<byte[]> columnFamiliesBytes;
    private final RocksDB db;
    private final ArrayList<ColumnFamilyHandle> columnFamilyHandles;

    public static RocksDBConnector getInstance(String path) {
        if(instance == null)
            instance = new RocksDBConnector(path);
        return instance;
    }

    public static RocksDBConnector getInstance() throws IllegalAccessException {
        if (instance == null)
            throw new IllegalAccessException("Can't access connector before specifying a path.");
        return instance;
    }

    private RocksDBConnector(String path){
        this.path = path;
        this.rocksObjects = new ArrayList<>();
        this.columnFamiliesBytes = new HashSet<>();
        this.columnFamilyHandles = new ArrayList<>();
        this.db = initialize();
    }


    private RocksDB initialize(){
        LOG.info("Initializing Database");
        RocksDB.loadLibrary();
        //Check if directory and file already exist, if not create them
        File f = new File(path);
        if (!f.exists()){
            try {
                Files.createDirectories(f.getParentFile().toPath());
                Files.createDirectories(f.getAbsoluteFile().toPath());
            } catch(IOException ex){
                ex.printStackTrace();
            }
        }

        ArrayList<ColumnFamilyDescriptor> cfDesc = new ArrayList<>();
        try{
            Options options = new Options();
            ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                    .optimizeLevelStyleCompaction();
            this.rocksObjects.add(options);
            this.columnFamiliesBytes.addAll(RocksDB.listColumnFamilies(options, this.path));
            if(this.columnFamiliesBytes.isEmpty())
                this.columnFamiliesBytes.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            for(byte[] colFam : this.columnFamiliesBytes){
                cfDesc.add(new ColumnFamilyDescriptor(colFam, cfOpts));
                LOG.info("Opening column family " + new String(colFam));
            }
        }catch (RocksDBException e){
            e.printStackTrace();
        }


        try {
            //TODO: DBOptions.getDBOptionsFromProps(new Properties());
            DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
            this.rocksObjects.add(options);
            return RocksDB.open(options, path, cfDesc, this.columnFamilyHandles);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }

    }

    public List<ColumnFamilyHandle> getColumnFamilyHandles(){
        return this.columnFamilyHandles;
    }


    public ColumnFamilyHandle registerColumnFamily(byte[] columnFamilyName) {
        LOG.info("Registering column family " + new String(columnFamilyName, StandardCharsets.UTF_8));
        try {
            for(ColumnFamilyHandle cfHandle : this.columnFamilyHandles){
                if(Arrays.equals(cfHandle.getName(), columnFamilyName)){
                    return cfHandle;
                }
            }
            ColumnFamilyHandle cfHandle = db.createColumnFamily(new
                    ColumnFamilyDescriptor(columnFamilyName, new ColumnFamilyOptions()
                    .optimizeUniversalStyleCompaction()
                    .optimizeForSmallDb()));
            this.columnFamilyHandles.add(cfHandle);
            this.columnFamiliesBytes.add(columnFamilyName);
            return cfHandle;
        } catch (RocksDBException e) {
            LOG.error("Could not register column family " + new String(columnFamilyName, StandardCharsets.UTF_8), e);
            e.printStackTrace();
        }
        return null;
    }


    public void deleteColumnFamily(ColumnFamilyHandle columnFamily) throws RocksDBException {
        this.columnFamiliesBytes.remove(columnFamily.getName());
        db.dropColumnFamily(columnFamily);
        columnFamily.close();
    }

    public void close(){
        LOG.info("Closing Database.");
        //close open column family handles
        for(ColumnFamilyHandle cfh : this.columnFamilyHandles){
            cfh.close();
        }
        this.columnFamilyHandles.clear();
        for(RocksObject ro : this.rocksObjects){
            ro.close();
        }
        this.rocksObjects.clear();
        //close database
        db.close();
    }

    public void put(byte[] key, byte[] value, ColumnFamilyHandle columnFamily) throws RocksDBException {
        db.put(columnFamily, key, value);
    }

    public byte[] get(byte[] key, ColumnFamilyHandle columnFamily) throws RocksDBException {
        return db.get(columnFamily, key);
    }

    public void delete(byte[] key, ColumnFamilyHandle columnFamily) throws RocksDBException {
        db.delete(columnFamily, key);
    }

}
