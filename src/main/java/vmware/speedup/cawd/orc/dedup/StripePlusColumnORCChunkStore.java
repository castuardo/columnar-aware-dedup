package vmware.speedup.cawd.orc.dedup;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import vmware.speedup.cawd.dedup.ColumnarChunkStore;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk;

public class StripePlusColumnORCChunkStore extends ColumnarChunkStore<StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk, StripePlusColumnORCChunkingAlgorithm>{

	private Map<StripePlusColumnORCFileChunk, StripePlusColumnORCFileChunk> chunkStore = new HashMap<StripePlusColumnORCFileChunk, StripePlusColumnORCFileChunk>();
	
	@Override
	public List<StripePlusColumnORCFileChunk> addChunks(byte[] data, StripePlusColumnORCChunkingAlgorithm algorithm) throws NoSuchAlgorithmException {
		// here, we will just add the whole chunk and its signature...
		byte[] signature = algorithm.naiveSHA1(data);
		StripePlusColumnORCFileChunk chunk = new StripePlusColumnORCFileChunk(signature, data);
		chunkStore.put(new StripePlusColumnORCFileChunk(signature), chunk);
		return Lists.newArrayList(chunk);
	}
	
	public List<StripePlusColumnORCFileChunk> addChunksWithLinks(StripePlusColumnORCFileChunk chunk, StripePlusColumnORCChunkingAlgorithm algorithm) throws NoSuchAlgorithmException {
		// here, we will just add the whole chunk and its signature...
		byte[] signature = algorithm.naiveSHA1(chunk.getContent());
		// the chunk here is expected to have links, aka subchunks
		chunkStore.put(new StripePlusColumnORCFileChunk(signature), chunk);
		return Lists.newArrayList(chunk);
	}

	@Override
	public StripePlusColumnORCFileChunk findChunkBySignature(byte[] signature) {
		return chunkStore.get(new StripePlusColumnORCFileChunk(signature));
	}

}
