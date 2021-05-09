package edu.aifb.rocksdb_restful;

import edu.aifb.rocksdb_restful.db.rocksdb.RocksDBConnector;
import edu.aifb.rocksdb_restful.db.rocksdb.RocksDBKeyValueStore;
import edu.aifb.rocksdb_restful.db.rocksdb.RocksDBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PreDestroy;

@SpringBootApplication
public class RocksdbRestfulApplication {

	private static final Logger LOG = LoggerFactory.getLogger(RocksdbRestfulApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(RocksdbRestfulApplication.class, args);
	}

	@PreDestroy
	public void onExit(){
		try {
			RocksDBConnector.getInstance().close();
		} catch (IllegalAccessException e) {
			LOG.error("Failed to close Database.", e);
		}
	}
}
