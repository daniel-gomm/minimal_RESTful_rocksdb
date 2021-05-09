package edu.aifb.rocksdb_restful.api;

import edu.aifb.rocksdb_restful.db.model.KeyValueStore;
import edu.aifb.rocksdb_restful.db.rocksdb.RocksDBManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DatabaseAccessController {

    private final RocksDBManager manager = RocksDBManager.getInstance();

    @PutMapping("/single/{column}")
    public void addColumn(@PathVariable byte[] column){
        manager.addKeyValueStore(column);
    }

    @DeleteMapping("/single/{column}")
    public void deleteColumn(@PathVariable byte[] column){
        manager.removeKeyValueStore(column);
    }

    @GetMapping("/single/{column}/{key}")
    public byte[] getEntry(@PathVariable byte[] column, @PathVariable byte[] key) throws IllegalAccessException {
        return manager.getKeyValueStore(column).get(key);
    }

    @PostMapping("/single/{column}/{key}")
    public void putEntry(@PathVariable byte[] column, @PathVariable byte[] key, @RequestBody byte[] value) throws IllegalAccessException {
        manager.getKeyValueStore(column).put(key, value);
    }

    @PutMapping("/single/{column}/{key}")
    public void updateEntry(@PathVariable byte[] column, @PathVariable byte[] key, @RequestBody byte[] value) throws IllegalAccessException {
        manager.getKeyValueStore(column).update(key, value);
    }

    @DeleteMapping("/single/{column}/{key}")
    public void deleteEntry(@PathVariable byte[] column, @PathVariable byte[] key) throws IllegalAccessException {
        manager.getKeyValueStore(column).delete(key);
    }


    @PostMapping(value = "/multiple/{column}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void putEntries(@PathVariable byte[] column,
                           @RequestBody HashMap<String, String> newEntries) throws IllegalAccessException {
        KeyValueStore<byte[], byte[]> keyValueStore = manager.getKeyValueStore(column);
        for(Map.Entry<String, String> entry : newEntries.entrySet()){
            keyValueStore.put(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
    }

    @GetMapping(value = "/multiple/{column}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, String> getEntries(@PathVariable byte[] column,
                           @RequestBody List<String> keys) throws IllegalAccessException {
        Map<String, String> ret = new HashMap<>();
        KeyValueStore<byte[], byte[]> keyValueStore = manager.getKeyValueStore(column);
        for(String key : keys){
            ret.put(key, new String(keyValueStore.get(key.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
        }
        return ret;
    }

}
