package vmware.speedup.cawd.orc.dedup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.LinkedList;

import vmware.speedup.cawd.dedup.ChunkingAlgorithm;
import com.eyalzo.common.chunks.PackChunking;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FileChunkingORCChunkingAlgorithm extends ChunkingAlgorithm<FileChunkingORCChunkingAlgorithm.ORCFileChunk> {
    private static final Logger logger = LogManager.getLogger(FileChunkingORCChunkingAlgorithm.class);
	
	@Override
	public List<ORCFileChunk> eagerChunking(String fileName) throws IOException {
		List<ORCFileChunk> identifiedChunks = new ArrayList<ORCFileChunk>();
        logger.debug("Open orc file: " + fileName);
        File tmp = new File(fileName);
        InputStream is = new FileInputStream(tmp);
        try {
            byte[] rawbytes = new byte[(int)tmp.length()];
            is.read(rawbytes, 0, rawbytes.length);

            LinkedList<Long> curChunkList = new LinkedList<Long>();
            PackChunking pack = new PackChunking(12);
            pack.getChunksAll(curChunkList, rawbytes, 0, rawbytes.length);

            long curPos = 0;
            int rawLenCumu = 0;
            for (Long curChunk : curChunkList) {
                int curChunkLen = PackChunking.chunkToLen(curChunk);
                identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Data, curPos, curChunkLen));
                curPos += curChunkLen;
                rawLenCumu += curChunkLen;
            }
            if(rawLenCumu != rawbytes.length){
                logger.error("PackChunking chunks total size {}, should be {}", rawLenCumu, rawbytes.length);
            }
        
            return identifiedChunks;
		}
		finally {
            if(is != null) is.close();
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
