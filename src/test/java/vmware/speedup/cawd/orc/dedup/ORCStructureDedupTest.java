package vmware.speedup.cawd.orc.dedup;

import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.junit.jupiter.api.Test;
import vmware.speedup.cawd.orc.dedup.NaiveORCChunkingAlgorithm.ORCFileChunk;

class ORCStructureDedupTest {

	private static final Logger logger = LogManager.getLogger(ORCStructureDedupTest.class);
	
	@Test
	void testNaiveORCChunking() {
		String fileName = "res/sample-1.orc";
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
		}
		catch(Exception e) {
			logger.error("Error while running test...", e);
		}
		finally {
			if(orcReader != null) {
				try {
					orcReader.close();
				}
				catch(IOException e) {}
			}
		}
	}

}
