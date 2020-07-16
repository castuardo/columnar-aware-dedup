package vmware.speedup.cawd.orc.dedup;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;

import vmware.speedup.cawd.dedup.ChunkingAlgorithm;


public class ColumnBasedORCChunkingAlgorithm extends ChunkingAlgorithm<ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk> {

	@Override
	public List<ColumnBasedORCFileChunk> eagerChunking(String fileName) throws IOException {
		List<ColumnBasedORCFileChunk> chunks = new ArrayList<ColumnBasedORCFileChunk>();
		Reader orcReader = null;
		RandomAccessFile rand = null;
		try {
			orcReader = OrcFile.createReader(new Path(fileName), OrcFile.readerOptions(new Configuration()));
			rand = new RandomAccessFile(fileName, "r");
			long lastStripeFooterEnds = 0;
			long currentStripeOffset = 0;
			for(StripeInformation stripe : orcReader.getStripes()) {
				// so, the indexes are regular chunks...
				chunks.add(new ColumnBasedORCFileChunk(
						ColumnBasedORCFileChunk.ChunkType.Regular, currentStripeOffset, stripe.getIndexLength()));
				// now, get the column chunks
				chunks.addAll(getStripeColumnChunks(stripe, getStripeFooter(rand, stripe), currentStripeOffset + stripe.getIndexLength()));
				// and get the footer...
				chunks.add(new ColumnBasedORCFileChunk(
						ColumnBasedORCFileChunk.ChunkType.Regular, 
						currentStripeOffset + stripe.getIndexLength() + stripe.getDataLength(), 
						stripe.getFooterLength()));
				lastStripeFooterEnds += stripe.getLength();
				currentStripeOffset = lastStripeFooterEnds;
			}
			// and get the rest of the file, footer and postscript...
			chunks.add(new ColumnBasedORCFileChunk(
					ColumnBasedORCFileChunk.ChunkType.Footer, lastStripeFooterEnds, rand.length() - lastStripeFooterEnds));
			// done...
			return chunks;
		}
		finally {
			if(rand != null) rand.close();
			if(orcReader != null) orcReader.close();
		}
	}

	private List<ColumnBasedORCFileChunk> getStripeColumnChunks(
			StripeInformation stripe, OrcProto.StripeFooter stripeFooter, long currentOffset){
		int streamCounter = 1;
		int processedColumns = 0;
		// the start offset is where the data starts in the file...
		List<ColumnBasedORCFileChunk> colChunks = new ArrayList<ColumnBasedORCFileChunk>();
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
				colChunks.add(new ColumnBasedORCFileChunk(ColumnBasedORCFileChunk.ChunkType.Column, currentOffset, columnLength));
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
	
	public static class ColumnBasedORCFileChunk extends ChunkingAlgorithm.Chunk {
		
		public static enum ChunkType {
			Column,
			Regular,
			Footer,
			ERROR
		}
		
		public static ChunkType fromOrdinal(int ordinal) {
			switch(ordinal) {
				case 1: return  ChunkType.Column;
				case 2: return  ChunkType.Regular;
				case 3: return  ChunkType.Footer;
				default: return ChunkType.ERROR;
			}
		}
		
		public static int toOrdinal(ChunkType type) {
			switch(type) {
				case Column: return 1;
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
		
		public ColumnBasedORCFileChunk(ChunkType type, long start, long size) {
			this.signature = null;
			this.content = null;
			this.type = type;
			this.start = start;
			this.size = size;
		}
		
		public ColumnBasedORCFileChunk(byte[] signature) {
			this.signature = signature;
		}
		
		public ColumnBasedORCFileChunk(byte[] signature, byte[] content) {
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
			builder.append("ColumnBasedORCFileChunk [type=").append(type.name()).append(", start=").append(start).append(", size=")
					.append(size).append(", signature=").append(signature != null? Arrays.toString(signature) : "none").append("]");
			return builder.toString();
		}

		@Override
		public int doHashCode() {
			return Arrays.hashCode(signature);
		}

		@Override
		public boolean doEquals(Object other) {
			if(other instanceof ColumnBasedORCFileChunk) {
				return Arrays.equals(signature, ((ColumnBasedORCFileChunk)other).signature); 
			}
			return false;
		}
		
	}

}
