package vmware.speedup.cawd.dedup;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;


public abstract class ChunkingAlgorithm<T extends ChunkingAlgorithm.Chunk> {
	
	public abstract List<T> eagerChunking(String fileName) throws IOException;
	
	public byte[] naiveSHA1(byte[] buffer) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-1"); 
		return md.digest(buffer);
	}
	
	public static abstract class Chunk {
		
		public abstract int doHashCode();
		public abstract boolean doEquals(Object other);
		
		@Override
		public int hashCode() {
			return doHashCode();
		}
		
		@Override
		public boolean equals(Object o) {
			return doEquals(o);
		}
		
	}
	
}
