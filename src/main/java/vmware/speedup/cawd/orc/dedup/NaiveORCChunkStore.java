package vmware.speedup.cawd.orc.dedup;

import java.util.List;
import vmware.speedup.cawd.dedup.ColumnarChunkStore;
import vmware.speedup.cawd.orc.dedup.NaiveORCChunkingAlgorithm.ORCFileChunk;

public class NaiveORCChunkStore extends ColumnarChunkStore<NaiveORCChunkingAlgorithm.ORCFileChunk, NaiveORCChunkingAlgorithm>{

	@Override
	public List<ORCFileChunk> findOrAddChunks(byte[] data, NaiveORCChunkingAlgorithm algorithm) {
		// TODO Auto-generated method stub
		return null;
	}

}
