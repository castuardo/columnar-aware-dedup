package vmware.speedup.cawd.orc.dedup;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import vmware.speedup.cawd.dedup.ColumnarChunkStore;
import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk;

public class ColumnBasedORCChunkStore extends ColumnarChunkStore<ColumnBasedORCFileChunk, ColumnBasedORCChunkingAlgorithm>{

	private Map<ColumnBasedORCFileChunk, ColumnBasedORCFileChunk> chunkStore = new HashMap<ColumnBasedORCFileChunk, ColumnBasedORCFileChunk>();
	
	@Override
	public List<ColumnBasedORCFileChunk> addChunks(byte[] data, ColumnBasedORCChunkingAlgorithm algorithm)
			throws NoSuchAlgorithmException {
		// here, we will just add the whole chunk and its signature...
		byte[] signature = algorithm.naiveSHA1(data);
		ColumnBasedORCFileChunk chunk = new ColumnBasedORCFileChunk(signature, data);
		chunkStore.put(new ColumnBasedORCFileChunk(signature), chunk);
		return Lists.newArrayList(chunk);
	}

	@Override
	public ColumnBasedORCFileChunk findChunkBySignature(byte[] signature) {
		return chunkStore.get(new ColumnBasedORCFileChunk(signature));
	}

}
