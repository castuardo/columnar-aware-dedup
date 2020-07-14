package vmware.speedup.cawd.orc.net;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.net.SpeedupStreamer;

public class ORCStreamer extends SpeedupStreamer {

	private static final Logger logger = LogManager.getLogger(ORCStreamer.class);
	
	private static class ORCFileChunk {
		
		public enum ChunkType {
			Data,
			Regular,
			Footer
		}
		
		private ChunkType type = null;
		private long start = 0;
		private long size = 0;
		
		public ORCFileChunk(ChunkType type, long start, long size) {
			this.type = type;
			this.start = start;
			this.size = size;
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

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ORCFileChunk [type=").append(type.name()).append(", start=").append(start).append(", size=")
					.append(size).append("]");
			return builder.toString();
		}
		
		
		
	}
	
	// we are going to identify the chunks that contain data only
	private List<ORCFileChunk> identifyDataChunks(String fileName) throws IOException {
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
					// this is the first none-special chunk...
					identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, 0, dataStartIndex));
					firstStripe = false;
				}
				else {
					// this is for the nth stripe, here i need the index
					identifiedChunks.add(new ORCFileChunk(ORCFileChunk.ChunkType.Regular, stripe.getOffset(), stripe.getIndexLength()));
				}
				// this is the row data
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
	
	@Override
	public TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException {
		try {
			List<ORCFileChunk> chunks = identifyDataChunks(fileName);
			logger.info("{}", Arrays.toString(chunks.toArray()));
			return null;
		}
		finally {
			
		}
	}
	
	
	

}
