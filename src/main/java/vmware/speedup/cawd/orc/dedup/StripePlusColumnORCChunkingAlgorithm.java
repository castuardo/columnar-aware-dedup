package vmware.speedup.cawd.orc.dedup;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import vmware.speedup.cawd.dedup.ChunkingAlgorithm;


public class StripePlusColumnORCChunkingAlgorithm extends ChunkingAlgorithm<StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk>{

	private static final Logger logger = LogManager.getLogger(StripePlusColumnORCChunkingAlgorithm.class);
	
	@Override
	public List<StripePlusColumnORCFileChunk> eagerChunking(String fileName)
			throws IOException {
		List<StripePlusColumnORCFileChunk> chunks = new ArrayList<StripePlusColumnORCFileChunk>();
		Reader orcReader = null;
		RandomAccessFile rand = null;
		try {
			orcReader = OrcFile.createReader(new Path(fileName), OrcFile.readerOptions(new Configuration()));
			rand = new RandomAccessFile(fileName, "r");
			long lastStripeFooterEnds = 0;
			long currentStripeOffset = 3;
			for(StripeInformation stripe : orcReader.getStripes()) {
				// so, the indexes are regular chunks...
				chunks.add(new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeIndex, currentStripeOffset, stripe.getIndexLength()));
				// get the whole stripe data
				StripePlusColumnORCFileChunk stripeChunk = getStripeDataChunk(stripe, currentStripeOffset);
				chunks.add(stripeChunk);
				// now, get the column chunks
				stripeChunk.subchunks.addAll(getStripeColumnChunks(getStripeFooter(rand, stripe), currentStripeOffset + stripe.getIndexLength()));
				// and get the footer...
				StripePlusColumnORCFileChunk footerChunk = getStripeDataFooterChunk(stripe, currentStripeOffset);
				chunks.add(footerChunk);
				lastStripeFooterEnds += stripe.getLength();
				currentStripeOffset = lastStripeFooterEnds;
			}
			// and get the rest of the file, footer and postscript...
			chunks.add(new StripePlusColumnORCFileChunk(
					StripePlusColumnORCFileChunk.ChunkType.FileFooter, lastStripeFooterEnds, rand.length() - lastStripeFooterEnds));
			// done...
			return chunks;
		}
		finally {
			if(rand != null) rand.close();
			if(orcReader != null) orcReader.close();
		}
	}
	
	private StripePlusColumnORCFileChunk getStripeDataChunk(StripeInformation stripe, long currentOffset) {
		long dataStarts = currentOffset + stripe.getIndexLength();
		long dataSize = stripe.getDataLength();
		return new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeData, dataStarts, dataSize);
	}
	
	private StripePlusColumnORCFileChunk getStripeDataFooterChunk(StripeInformation stripe, long currentOffset) {
		long dataStarts = currentOffset + stripe.getIndexLength() + stripe.getDataLength();
		long dataSize = stripe.getFooterLength();
		return new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeFooter, dataStarts, dataSize);
	}
	
	public List<StripePlusColumnORCFileChunk> getStripeColumnChunks(OrcProto.StripeFooter stripeFooter, long currentOffset){
		int streamCounter = 1;
		int processedColumns = 0;
		// the start offset is where the data starts in the file...
		List<StripePlusColumnORCFileChunk> colChunks = new ArrayList<StripePlusColumnORCFileChunk>();
		for(int i = 0; i < stripeFooter.getStreamsCount(); ++i) {
			// these are index streams, we dont care...
			if(streamCounter <= stripeFooter.getColumnsCount()) {
				++streamCounter;
				continue;
			}
			// here, we have the streams related to each column. So we just keep moving forward until the column 
			// id changes
			if(processedColumns < stripeFooter.getColumnsCount()) {
				// currently, we are at the first piece of a column
				int column = stripeFooter.getStreamsList().get(i).getColumn();
				long columnLength = 0;
				while(true) {
					columnLength += stripeFooter.getStreamsList().get(i).getLength();
					++i;
					if(i >= stripeFooter.getStreamsList().size()) {
						// we are done, we got to the end
						break;
					}
					else if(stripeFooter.getStreamsList().get(i).getColumn() != column) {
						// we are done with this column. i is in the position on the first 
						// element of the next column, and is going to be incremented in the next 
						// loop, so we decrement it...
						i--;
						break;
					}
				}
				// we have the whole column size, so add it
				colChunks.add(new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.Column, currentOffset, columnLength));
				currentOffset += columnLength;
				++processedColumns;
			}
		}
		return colChunks;
	}
	
	// My understanding is that seek has constant complexity, so we should be fine. The cost of this method is basically 
	// protobuff parsing
	private OrcProto.StripeFooter getStripeFooter(RandomAccessFile rand, StripeInformation stripe) throws IOException {
		long footerOffset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
		rand.seek(footerOffset);
		byte[] stripeFooter = new byte[(int)stripe.getFooterLength()];
		rand.read(stripeFooter, 0, (int)stripe.getFooterLength());
		OrcProto.StripeFooter footer = OrcProto.StripeFooter.parseFrom(stripeFooter);
		return footer;
	}
	
	
	public static class StripePlusColumnORCFileChunk extends ChunkingAlgorithm.Chunk {
		
		public static enum ChunkType {
			Stripe,
			StripeIndex,
			StripeData,
			Column,
			StripeFooter,
			FileFooter,
			ColumnQuery,
			SmallColumn,
			ERROR
		}
		
		public static ChunkType fromOrdinal(int ordinal) {
			switch(ordinal) {
				case 1: return  ChunkType.Stripe;
				case 2: return  ChunkType.StripeIndex;
				case 3: return  ChunkType.StripeData;
				case 4: return  ChunkType.Column;
				case 5: return  ChunkType.StripeFooter;
				case 6: return  ChunkType.FileFooter;
				case 7: return  ChunkType.ColumnQuery;
				case 8: return  ChunkType.SmallColumn;
				default: return ChunkType.ERROR;
			}
		}
		
		public static int toOrdinal(ChunkType type) {
			switch(type) {
				case Stripe: return 1;
				case StripeIndex: return 2;
				case StripeData: return 3;
				case Column: return 4;
				case StripeFooter: return 5;
				case FileFooter: return 6;
				case ColumnQuery: return 7;
				case SmallColumn: return 8;
				default: return -1000;
			}
		}
		
		private byte[] signature = null;
		private byte[] content = null;
		private ChunkType type = null;
		private long start = 0;
		private long size = 0;
		private List<StripePlusColumnORCFileChunk> subchunks = new ArrayList<StripePlusColumnORCFileChunk>();
		
		public StripePlusColumnORCFileChunk(ChunkType type, long start, long size) {
			this.signature = null;
			this.content = null;
			this.type = type;
			this.start = start;
			this.size = size;
		}
		
		public StripePlusColumnORCFileChunk(byte[] signature) {
			this.signature = signature;
		}
		
		public StripePlusColumnORCFileChunk(byte[] signature, byte[] content) {
			this.signature = signature;
			this.content = content;
		}
		
		public StripePlusColumnORCFileChunk(ChunkType type, byte[] content) {
			this.type = type;
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

		public List<StripePlusColumnORCFileChunk> getSubchunks() {
			return subchunks;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("StripePlusColumnORCFileChunk [type=").append(type.name()).append(", start=").append(start).append(", size=")
					.append(size).append(", signature=").append(signature != null? Arrays.toString(signature) : "none")
					.append(", subchunks=").append(Arrays.toString(subchunks.toArray())).append("]");
			return builder.toString();
		}

		@Override
		public int doHashCode() {
			return Arrays.hashCode(signature);
		}

		@Override
		public boolean doEquals(Object other) {
			if(other instanceof StripePlusColumnORCFileChunk) {
				return Arrays.equals(signature, ((StripePlusColumnORCFileChunk)other).signature); 
			}
			return false;
		}
		
	}
	
}
