package vmware.speedup.cawd.alluxio;

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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.AbstractMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vmware.speedup.cawd.alluxio.Cache;
import vmware.speedup.cawd.alluxio.ByteArrayWrapper;
// import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm;
// import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm.ParquetFileChunk;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm.ParquetFileChunk;

public class ChunkCache extends Cache {
	private static final Logger logger = LogManager.getLogger(ChunkCache.class);
    
    private Set<String> fileStore;
    // signature -> <chunkLen, referenceNum>
    private Map<ByteArrayWrapper, AbstractMap.SimpleEntry<Long, Integer>> chunkStore;
    private PageChunkingParquetChunkingAlgorithm chunkingAlgo;

    public ChunkCache() {
        super();
        fileStore = new HashSet<String>();
        chunkStore = new HashMap<ByteArrayWrapper, AbstractMap.SimpleEntry<Long, Integer>>();
        chunkingAlgo = new PageChunkingParquetChunkingAlgorithm();
    }
    @Override
    public void readFile(String path) throws IOException {
        List<ParquetFileChunk> chunks = chunkingAlgo.eagerChunking(path);
        for(ParquetFileChunk chunk : chunks){
            ByteArrayWrapper signature = new ByteArrayWrapper(chunk.getSignature());
            if(chunkStore.containsKey(signature)){
                long chunkLen = chunkStore.get(signature).getKey();
                int referenceNum = chunkStore.get(signature).getValue();
                if(fileStore.contains(path)){
                    // chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum));
                }
                else{
                    chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum+1));
                }
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
            ByteArrayWrapper signature = new ByteArrayWrapper(chunk.getSignature());
            if(chunkStore.containsKey(signature)){
                long chunkLen = chunkStore.get(signature).getKey();
                int referenceNum = chunkStore.get(signature).getValue();
                if(fileStore.contains(path)){
                    // chunkStore.put(signature, new AbstractMap.SimpleEntry<>(chunkLen, referenceNum));
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
        List<ParquetFileChunk> chunks = chunkingAlgo.eagerChunking(path);
        for(ParquetFileChunk chunk : chunks){
            ByteArrayWrapper signature = new ByteArrayWrapper(chunk.getSignature());
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
                logger.error("evicted file chunk not in chunkstore");
                return;
            }
        }
        fileStore.remove(path);
    }
}
