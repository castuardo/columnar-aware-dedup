package vmware.speedup.cawd.orc.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;

import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk.ChunkType;

class ORCStructureDedupTest {

	private static final Logger logger = LogManager.getLogger(ORCStructureDedupTest.class);
	
	@Test
	void doTest() throws IOException {
		try {
			ORCGroupAndWriteBolt x = new ORCGroupAndWriteBolt();
			x.run();
		}
		catch(Exception e) {
			logger.error(e);
		}
	}
	
    public static class ORCGroupAndWriteBolt  {
        private Map<String, Map<String, Integer>> counts = new HashMap<String, Map<String, Integer>>();
        private long windowId = 0;
        
        
        public void run() {
        	Random random = new Random(System.currentTimeMillis());
        	for(int k = 0; k < 30; ++k) {
	        	for(int i = 1; i <= 60000; ++i) {
	        		execute("Identifier" + random.nextInt(100), "Identifier" + random.nextInt(100), String.valueOf(System.currentTimeMillis()), i);
	        	}
        	}
        }
        

        public void execute(String adId, String type, String time, int count) {
            aggregate(adId, type);
            if(!validateTimeWindow(count)) {
            	for(Entry<String, Map<String, Integer>> entry : counts.entrySet()) {
            		try {
            			writeTimeWindowForType(entry.getKey(), entry.getValue());
            		}
            		catch(Exception e) {
            			System.out.println("ERROR HERE!");
            			e.printStackTrace(System.out);
            		}
            	}
            	++windowId;
            }
        }

        private boolean validateTimeWindow(int numEntries) {
        	if(numEntries >= 60000) {
        		return false;
        	}
        	else {
        		return true;
        	}
        }
        
        private Writer buildWriter(String adId, long windowId) throws IOException {
        	OrcFile.WriterOptions options = OrcFile.writerOptions(new Configuration())
        			.compress(CompressionKind.NONE).setSchema(TypeDescription.fromString("struct<campaign:string,type:string,count:int>"));
        	return OrcFile.createWriter(new Path("/tmp/events/" + adId + "-" + windowId + ".orc"), options);
        }
        
        private void aggregate(String ad, String type) {
        	Map<String, Integer> inner = counts.get(ad);
        	if(inner == null) {
        		inner = new HashMap<String, Integer>();
        		inner.put(type, 1);
        		counts.put(ad, inner);
        	}
        	else {
        		Integer cc = inner.get(type);
        		if(cc == null) {
        			inner.put(type, 1);
        		}
        		else {
        			inner.put(type, cc + 1);
        		}
        	}
        }
        
        private void writeTimeWindowForType(String type, Map<String, Integer> counts) throws IOException {
        	if(counts.size() > 0) {
	        	Writer writer = buildWriter(type, windowId);
	        	VectorizedRowBatch batch = writer.getSchema().createRowBatch();
	        	BytesColumnVector x = (BytesColumnVector) batch.cols[0];
	        	BytesColumnVector y = (BytesColumnVector) batch.cols[1];
	        	LongColumnVector z = (LongColumnVector) batch.cols[2];
	        	int current = 0 ;
	        	for(Entry<String, Integer> entry : counts.entrySet()) {
	        		x.vector[current] = type.getBytes();
	        		y.vector[current] = entry.getKey().getBytes();
	        		z.vector[current] = entry.getValue();
	        		++current;
	        	}
	        	writer.addRowBatch(batch);
	        	writer.close();
        	}
        }
    }
}
