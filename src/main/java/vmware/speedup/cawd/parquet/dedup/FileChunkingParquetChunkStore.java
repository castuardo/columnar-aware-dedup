package vmware.speedup.cawd.parquet.dedup;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import vmware.speedup.cawd.dedup.ColumnarChunkStore;
import vmware.speedup.cawd.parquet.dedup.FileChunkingParquetChunkingAlgorithm.ParquetFileChunk;

public class FileChunkingParquetChunkStore extends ColumnarChunkStore<FileChunkingParquetChunkingAlgorithm.ParquetFileChunk, FileChunkingParquetChunkingAlgorithm>{

	private Map<ParquetFileChunk, ParquetFileChunk> chunkStore = new HashMap<ParquetFileChunk, ParquetFileChunk>();
	
	@Override
	public List<ParquetFileChunk> addChunks(byte[] data, FileChunkingParquetChunkingAlgorithm algorithm) throws NoSuchAlgorithmException {
		// here, we will just add the whole chunk and its signature...
		byte[] signature = algorithm.naiveSHA1(data);
		ParquetFileChunk chunk = new ParquetFileChunk(signature, data);
		chunkStore.put(new ParquetFileChunk(signature), chunk);
		return Lists.newArrayList(chunk);
	}

	@Override
	public ParquetFileChunk findChunkBySignature(byte[] signature) {
		return chunkStore.get(new ParquetFileChunk(signature));
	}

}
