package vmware.speedup.cawd.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm;
// import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm.ParquetFileChunk;

import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm.ParquetFileChunk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.AbstractMap;

import static org.apache.parquet.cli.Util.humanReadable;
import org.apache.commons.io.FileUtils;

public class AlluxioSim {
	private static final Logger logger = LogManager.getLogger(AlluxioSim.class);

    private static String pathLocalize(String hdfsUrl){
        return hdfsUrl.replace("hdfs://node0:9000/user/hive/warehouse", "/home/administrator/spark-tpc-ds-performance-test/parquetdata");
    }

    public abstract class Cache {
        public long ufsReadBytes;
        public long ufsWriteBytes;
        public long cacheSize;
        public Cache() {
            ufsReadBytes = 0;
            ufsWriteBytes = 0;
            cacheSize = 0;
        }
        public void readFile(String path) throws IOException {
            
        }
        public void writeFile(String path) throws IOException {

        }
        public void evictFile(String path) throws IOException {
            
        }
        public String toString() {
            return new StringBuilder()
            .append("ufsReadBytes="+ufsReadBytes) 
            .append("ufsWriteBytes="+ufsWriteBytes) 
            .append("cacheSize="+cacheSize) 
            // .append(String.format("; dedup ratio: %.3f%%, extra/totalTransfer = %.3f%%", (1 - totalTransSize / allFileSize) * 100, extraTransSize / totalTransSize * 100))
            .toString();
        }
    }
    public class FileCache extends Cache {
        public FileCache() {
            
        }
        @Override
        public void readFile(String path) throws IOException {
        
        
        }
        @Override
        public void writeFile(String path) throws IOException {

            
        }
        @Override
        public void evictFile(String path) throws IOException {

            
        }
    }

    public class ChunkCache extends Cache {
        private Set<String> fileStore;
        // signature -> <chunkLen, referenceNum>
        private Map<byte[], AbstractMap.SimpleEntry<Long, Integer>> chunkStore;
        private PageChunkingParquetChunkingAlgorithm chunkingAlgo;

        public ChunkCache() {
            super();
            fileStore = new HashSet<String>();
            chunkStore = new HashMap<byte[], AbstractMap.SimpleEntry<Long, Integer>>();
            chunkingAlgo = new PageChunkingParquetChunkingAlgorithm();
        }
        @Override
        public void readFile(String path) throws IOException {
            List<ParquetFileChunk> chunks = chunkingAlgo.eagerChunking(path);
            for(ParquetFileChunk chunk : chunks){
                byte[] signature = chunk.getSignature();
                if(chunkStore.containsKey(signature)){
                    long chunkLen = chunkStore.get(signature).getKey();
                    int referenceNum = chunkStore.get(signature).getValue();
                    chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum+1));
                }
                else{
                    chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunk.getSize(), 1));
                    ufsReadBytes += chunk.getSize();
                    cacheSize += chunk.getSize();
                }
            }
            fileStore.add(path);
        }
        @Override
        public void writeFile(String path) throws IOException {
            List<ParquetFileChunk> chunks = chunkingAlgo.eagerChunking(path);
            for(ParquetFileChunk chunk : chunks){
                byte[] signature = chunk.getSignature();
                if(chunkStore.containsKey(signature)){
                    long chunkLen = chunkStore.get(signature).getKey();
                    int referenceNum = chunkStore.get(signature).getValue();
                    if(fileStore.contains(path)){
                        chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum));
                    }
                    else{
                        chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum+1));
                    }
                }
                else{
                    chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunk.getSize(), 1));
                    ufsWriteBytes += chunk.getSize();
                    cacheSize += chunk.getSize();
                }
            }
            fileStore.add(path);
        }
        @Override
        public void evictFile(String path) throws IOException {
            if(!fileStore.contains(path)){
                logger.error("evicted file not in filestore");
                return;
            }
            fileStore.remove(path);
            List<ParquetFileChunk> chunks = chunkingAlgo.eagerChunking(path);
            for(ParquetFileChunk chunk : chunks){
                byte[] signature = chunk.getSignature();
                if(chunkStore.containsKey(signature)){
                    long chunkLen = chunkStore.get(signature).getKey();
                    int referenceNum = chunkStore.get(signature).getValue();
                    if(referenceNum == 1){
                        chunkStore.remove(signature);
                        cacheSize -= chunk.getSize();
                    }
                    else{
                        chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum-1));
                    }
                }
                else{
                    logger.error("evicted file not in chunkstore");
                }
            }
        }
    }
    public void run(String... args) throws IOException{
        byte[] rawdata = Files.readAllBytes(Paths.get("/home/administrator/spark-tpc-ds-performance-test/log/trimed.log"));
        Map<String, List<String>> logsMap = new ObjectMapper().readValue(rawdata, TreeMap.class);
        
        List<Cache> caches = new ArrayList<Cache>();
        caches.add(new ChunkCache());
        caches.add(new ChunkCache());
        caches.add(new ChunkCache());
        caches.add(new ChunkCache());
        
        List<String> lastList = null;
        for (Map.Entry<String, List<String>> entry : logsMap.entrySet()) {
            String time = entry.getKey();
            List<String> curList = entry.getValue();
            logger.info(time + ": " + curList.toString());
            
            String op = curList.get(0);
            int cacheIdx = Integer.parseInt(curList.get(1));
            String path = pathLocalize(curList.get(2));
            
            if(!curList.equals(lastList)){
                System.out.println(entry.getValue());
                // read, write, evict.
                if(op == "readfromUfs"){
                    caches.get(cacheIdx-1).readFile(path);
                }
                else if(op == "evictBlock"){
                    caches.get(cacheIdx-1).evictFile(path);
                }
                else{
                    caches.get(cacheIdx-1).writeFile(path);
                }
            }
            lastList = entry.getValue();
        }

        for(Cache c : caches){
            logger.info(c.toString());
        }
    }

	public static void main(String... args) {
        try {
            AlluxioSim alluxiosim = new AlluxioSim();
            alluxiosim.run(args);
        } catch (IOException e) {
            logger.error("Alluxio Sim error");
        }
	}
}
