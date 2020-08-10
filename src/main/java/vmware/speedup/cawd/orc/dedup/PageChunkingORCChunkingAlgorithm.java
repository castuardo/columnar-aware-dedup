package vmware.speedup.cawd.orc.dedup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;

import com.eyalzo.common.chunks.ChunksFiles;
import com.eyalzo.common.chunks.PackChunking;
import vmware.speedup.cawd.dedup.ChunkingAlgorithm;

public class PageChunkingORCChunkingAlgorithm extends ChunkingAlgorithm<PageChunkingORCChunkingAlgorithm.ORCFileChunk> {
    private static final Logger logger = LogManager.getLogger(PageChunkingORCChunkingAlgorithm.class);
	private static final int FILE_BLOCK_SIZE = 1000000;
	
	@Override
	public List<ORCFileChunk> eagerChunking(String fileName) throws IOException {
		Reader orcReader = null;
		List<ORCFileChunk> identifiedChunks = new ArrayList<ORCFileChunk>();
        ByteBuffer buffer = ByteBuffer.allocate(FILE_BLOCK_SIZE);
        PackChunking pack = new PackChunking(12);
                
        try {
            FileInputStream fis = new FileInputStream(fileName);
    		byte[] allbytes = new byte[fis.available()];
    		fis.read(allbytes);

            byte[] rawbytes;
            long curPos = 0;
		
			orcReader = OrcFile.createReader(new Path(fileName), OrcFile.readerOptions(new Configuration()));
			boolean firstStripe = true;
			long lastStipeFooterEnds = 0;
			for(StripeInformation stripe : orcReader.getStripes()) {
				long dataStartIndex = stripe.getOffset() + stripe.getIndexLength();
				long dataSize = stripe.getDataLength();
				if(firstStripe) {
					// this is the first non-special chunk...
					// identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, 0, dataStartIndex));
                    rawbytes = Arrays.copyOfRange(allbytes,  0, (int)dataStartIndex);
                    curPos = PackChunkingWrapper(pack, rawbytes, identifiedChunks, buffer, curPos, ORCFileChunk.ChunkType.Regular);
                    firstStripe = false;
				}
				else {
					// this is for the nth stripe, here i need the index
					// identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, stripe.getOffset(), stripe.getIndexLength()));
                    rawbytes = Arrays.copyOfRange(allbytes, (int)stripe.getOffset(), (int)stripe.getIndexLength());
                    curPos = PackChunkingWrapper(pack, rawbytes, identifiedChunks, buffer, curPos, ORCFileChunk.ChunkType.Regular);
                }
				// this is the row data. No hashing yet...
                // identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Data, dataStartIndex, dataSize));
                rawbytes = Arrays.copyOfRange(allbytes, (int)dataStartIndex, (int)dataSize);
                curPos = PackChunkingWrapper(pack, rawbytes, identifiedChunks, buffer, curPos, ORCFileChunk.ChunkType.Data);

                // and this is the footer
				// identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, dataStartIndex + dataSize, stripe.getFooterLength()));
                rawbytes = Arrays.copyOfRange(allbytes, (int)dataStartIndex + (int)dataSize, (int)stripe.getFooterLength());
                curPos = PackChunkingWrapper(pack, rawbytes, identifiedChunks, buffer, curPos, ORCFileChunk.ChunkType.Regular);

                lastStipeFooterEnds = dataStartIndex + dataSize + stripe.getFooterLength();
			}
			// identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Footer, lastStipeFooterEnds, new File(fileName).length() - lastStipeFooterEnds));
            rawbytes = Arrays.copyOfRange(allbytes, (int)lastStipeFooterEnds, (int)(new File(fileName).length() - lastStipeFooterEnds));
            curPos = PackChunkingWrapper(pack, rawbytes, identifiedChunks, buffer, curPos, ORCFileChunk.ChunkType.Footer);

            return identifiedChunks;
		}
		finally {
			if(orcReader != null) orcReader.close();
		}
	}
	// return the updated curPos
    public long PackChunkingWrapper(PackChunking pack, byte[] rawbytes, List<ORCFileChunk> identifiedChunks, ByteBuffer buffer, long curPos, ORCFileChunk.ChunkType chunkType){
        int rawLenCumu = 0;
        LinkedList<Long> curChunkList = new LinkedList<Long>();
        pack.getChunksAll(curChunkList, rawbytes, 0, rawbytes.length);
        for (Long curChunk : curChunkList) {
            int curChunkLen = PackChunking.chunkToLen(curChunk);
            ORCFileChunk chunk = new ORCFileChunk(chunkType, curPos, curChunkLen);
            try{
                chunk.setSignature(naiveSHA1(Arrays.copyOfRange(rawbytes, rawLenCumu, rawLenCumu + curChunkLen)));
            }
            catch(Exception e){
                logger.error(e.toString());
            }

            identifiedChunks.add(chunk);
            curPos += curChunkLen;
            rawLenCumu += curChunkLen;
        }
        if(rawLenCumu != rawbytes.length){
            logger.error("PackChunking chunks total size {}, should be {}", rawLenCumu, rawbytes.length);
        }
        return curPos;
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
