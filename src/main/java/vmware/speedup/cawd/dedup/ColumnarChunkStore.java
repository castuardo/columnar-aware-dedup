package vmware.speedup.cawd.dedup;

import java.security.NoSuchAlgorithmException;
import java.util.List;

public abstract class ColumnarChunkStore <T extends ChunkingAlgorithm.Chunk, K extends ChunkingAlgorithm<T>> {
	
	public abstract List<T> addChunks(byte[] data, K algorithm) throws NoSuchAlgorithmException;
	
	public abstract T findChunkBySignature(byte[] signature);
	
}
