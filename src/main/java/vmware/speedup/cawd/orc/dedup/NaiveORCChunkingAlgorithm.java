package vmware.speedup.cawd.orc.dedup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import vmware.speedup.cawd.dedup.ChunkingAlgorithm;


public class NaiveORCChunkingAlgorithm extends ChunkingAlgorithm<NaiveORCChunkingAlgorithm.ORCFileChunk> {
	
	@Override
	public List<ORCFileChunk> eagerChunking(String fileName) throws IOException {
		Reader orcReader = null;
		List<ORCFileChunk> identifiedChunks = new ArrayList<ORCFileChunk>();
		try {
			orcReader = OrcFile.createReader(new Path(fileName), OrcFile.readerOptions(new Configuration()));
			boolean firstStripe = true;
			long lastStipeFooterEnds = 0;
			for(StripeInformation stripe : orcReader.getStripes()) {
				long dataStartIndex = stripe.getOffset() + stripe.getIndexLength();
				long dataSize = stripe.getDataLength();
				if(firstStripe) {
					// this is the first non-special chunk...
					identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, 0, dataStartIndex));
					firstStripe = false;
				}
				else {
					// this is for the nth stripe, here i need the index
					identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, stripe.getOffset(), stripe.getIndexLength()));
				}
				// this is the row data. No hashing yet...
				identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Data, dataStartIndex, dataSize));
				// and this is the footer
				identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, dataStartIndex + dataSize, stripe.getFooterLength()));
				lastStipeFooterEnds = dataStartIndex + dataSize + stripe.getFooterLength();
			}
			identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Footer, lastStipeFooterEnds, new File(fileName).length() - lastStipeFooterEnds));
			return identifiedChunks;
		}
		finally {
			if(orcReader != null) orcReader.close();
		}
	}
	
	public static class ORCFileChunk extends ChunkingAlgorithm.Chunk {
		
		public static enum ChunkType {
			Data,
			Regular,
			Footer,
			ERROR
		}
		
		public static ChunkType fromOrdinal(int ordinal) {
			switch(ordinal) {
				case 1: return  ChunkType.Data;
				case 2: return  ChunkType.Regular;
				case 3: return  ChunkType.Footer;
				default: return ChunkType.ERROR;
			}
		}
		
		public static int toOrdinal(ChunkType type) {
			switch(type) {
				case Data: return 1;
				case Regular: return  2;
				case Footer: return 3;
				default: return -1000;
			}
		}
		
		private byte[] signature = null;
		private byte[] content = null;
		private ChunkType type = null;
		private long start = 0;
		private long size = 0;
		
		public ORCFileChunk(ChunkType type, long start, long size) {
			this.signature = null;
			this.content = null;
			this.type = type;
			this.start = start;
			this.size = size;
		}
		
		public ORCFileChunk(byte[] signature) {
			this.signature = signature;
		}
		
		public ORCFileChunk(byte[] signature, byte[] content) {
			this.signature = signature;
			this.content = content;
		}

		public ChunkType getType() {
			return type;
		}

		public long getStart() {
			return start;
		}

		public long getSize() {
			return size;
		}

		public byte[] getSignature() {
			return signature;
		}

		public byte[] getContent() {
			return content;
		}

		public void setSignature(byte[] signature) {
			this.signature = signature;
		}

		public void setContent(byte[] content) {
			this.content = content;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ORCFileChunk [type=").append(type.name()).append(", start=").append(start).append(", size=")
					.append(size).append(", signature=").append(signature != null? Arrays.toString(signature) : "none").append("]");
			return builder.toString();
		}

		@Override
		public int doHashCode() {
			return Arrays.hashCode(signature);
		}

		@Override
		public boolean doEquals(Object other) {
			if(other instanceof ORCFileChunk) {
				return Arrays.equals(signature, ((ORCFileChunk)other).signature); 
			}
			return false;
		}
		
	}
	
	
}
