package vmware.speedup.cawd.orc.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.junit.jupiter.api.Test;

import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk;

class ORCStructureDedupTest {

	private static final Logger logger = LogManager.getLogger(ORCStructureDedupTest.class);
	
	private String [] files = new String [] {"/home/castuardo/Desktop/none-all-orc/db1-10g-q-01-p0.orc"};
	
	private OrcProto.StripeFooter getStripeFooter(RandomAccessFile rand, StripeInformation stripe) throws IOException {
		long footerOffset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
		rand.seek(footerOffset);
		byte[] stripeFooter = new byte[(int)stripe.getFooterLength()];
		rand.read(stripeFooter, 0, (int)stripe.getFooterLength());
		OrcProto.StripeFooter footer = OrcProto.StripeFooter.parseFrom(stripeFooter);
		return footer;
	}
	
	private OrcProto.RowIndex getStripeIndex(RandomAccessFile rand, StripeInformation stripe) throws IOException {
		long offset = stripe.getOffset();
		rand.seek(offset);
		byte[] index = new byte[(int)stripe.getIndexLength()];
		rand.read(index, 0, index.length);
		OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(index);
		logger.info(rowIndex.getEntry(1).getPositionsList());
		return rowIndex;
	}
	
	@Test
	void doTest() throws IOException {
		for(String ff : files) {
			logger.info("File={}", ff);
			checkIndexAndFooter(ff);
			logger.info("-----------------------------------------------");
		}
	}
	
	private void checkIndexAndFooter(String fileName) throws IOException {
		Reader orcReader = null;
		RandomAccessFile rand = null;
		try {
			orcReader = OrcFile.createReader(new Path(fileName), OrcFile.readerOptions(new Configuration()));
			rand = new RandomAccessFile(fileName, "r");;
			for(StripeInformation stripe : orcReader.getStripes()) {
				OrcProto.StripeFooter ff = getStripeFooter(rand, stripe);
				OrcProto.RowIndex ii = getStripeIndex(rand, stripe);
				logger.info(ii);
				logger.info("-----");
				logger.info(ff);
				logger.info("-----");
				logger.info(orcReader.getSchema());
			}
		}
		finally {
			if(rand != null) rand.close();
			if(orcReader != null) orcReader.close();
		}
	}
	
}
