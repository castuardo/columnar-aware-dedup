package vmware.speedup.cawd.orc.dedup;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import vmware.speedup.cawd.dedup.ColumnarChunkStore;
import vmware.speedup.cawd.orc.dedup.PageChunkingORCChunkingAlgorithm.ORCFileChunk;

public class PageChunkingORCChunkStore extends ColumnarChunkStore<PageChunkingORCChunkingAlgorithm.ORCFileChunk, PageChunkingORCChunkingAlgorithm>{

	private Map<ORCFileChunk, ORCFileChunk> chunkStore = new HashMap<ORCFileChunk, ORCFileChunk>();
	
	@Override
	public List<ORCFileChunk> addChunks(byte[] data, PageChunkingORCChunkingAlgorithm algorithm) throws NoSuchAlgorithmException {
		// here, we will just add the whole chunk and its signature...
		byte[] signature = algorithm.naiveSHA1(data);
		ORCFileChunk chunk = new ORCFileChunk(signature, data);
		chunkStore.put(new ORCFileChunk(signature), chunk);
		return Lists.newArrayList(chunk);
	}

	@Override
	public ORCFileChunk findChunkBySignature(byte[] signature) {
		return chunkStore.get(new ORCFileChunk(signature));
	}

}
