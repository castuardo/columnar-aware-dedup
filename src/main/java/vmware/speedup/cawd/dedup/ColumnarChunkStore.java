package vmware.speedup.cawd.dedup;

import java.util.List;

public abstract class ColumnarChunkStore <T extends ChunkingAlgorithm.Chunk, K extends ChunkingAlgorithm<T>> {
	
	public abstract List<T> findOrAddChunks(byte[] data, K algorithm);
	
}
