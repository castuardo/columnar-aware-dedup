package vmware.speedup.cawd.orc.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk;

class ORCStructureDedupTest {

	private static final Logger logger = LogManager.getLogger(ORCStructureDedupTest.class);
	
	private String [] files = new String [] {"res/sample-1.orc", 
			"res/sample-2.orc"};
	
	
	@Test
	void testColumnChunking() {
		ColumnBasedORCChunkingAlgorithm algo = new ColumnBasedORCChunkingAlgorithm();
		for(String file : files) {
			try {
				File ff = new File(file);
				long len = ff.length();
				long totalSize = 0;
				List<ColumnBasedORCFileChunk> chunks = algo.eagerChunking(file);
				for(ColumnBasedORCFileChunk cc : chunks) {
					totalSize += cc.getSize();
				}
				logger.info("len={}, totalSize={}, eq={}", len, totalSize, len == totalSize);
				logger.info(chunks);
				assertEquals(len, totalSize);
			}
			catch(IOException e) {
				logger.error("Error processing {}", file, e);
			}
			logger.info("--------------------------------");
		}
	}
	
}
