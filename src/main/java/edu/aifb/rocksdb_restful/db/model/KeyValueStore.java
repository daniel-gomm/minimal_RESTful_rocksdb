package edu.aifb.rocksdb_restful.db.model;

public interface KeyValueStore<K, V> {

    boolean put(K key,V value);
    boolean update(K key,V value);
    V get(K key);
    boolean delete(K key);

}
