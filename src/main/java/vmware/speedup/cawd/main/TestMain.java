package vmware.speedup.cawd.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm;
import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm.ParquetFileChunk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import static org.apache.parquet.cli.Util.humanReadable;
import org.apache.commons.io.FileUtils;

public class TestMain {

	private static final Logger logger = LogManager.getLogger(TestMain.class);
	// buffer size used for reading and writing
    private static final int BUFFER_SIZE = 8192;


    /**
      * Reads all bytes from an input stream and writes them to an output stream.
      */
    private static long copy(InputStream source, OutputStream sink) throws IOException {
        long nread = 0L;
        byte[] buf = new byte[BUFFER_SIZE];
        int n;
        while ((n = source.read(buf)) > 0) {
            sink.write(buf, 0, n);
            nread += n;
        }
        return nread;
    }
    
	public static void main(String... args) {
		logger.info("Works fine ({})!", new Date().toString());
        logger.info("Works fine ({})!", new Date().toString());
        String testParquetFile = "/home/administrator/spark-tpc-ds-performance-test/src/mixdata/parquet/catalog_sales05g.snappy.parquet";
        int total_chunk_size = 0;
        try {
            List<ParquetFileChunk> chunks = new NaiveParquetChunkingAlgorithm().eagerChunking(testParquetFile);
            File f = new File(testParquetFile + ".parsed");
            OutputStream fos = new FileOutputStream(f, false); // not appending

            for(ParquetFileChunk chunk : chunks){
                total_chunk_size += chunk.getSize();
                byte[] rawbytes = chunk.getContent();
                fos.write(rawbytes);
                logger.info("total chunk size in bytes: " + Integer.toString(total_chunk_size));
            }

            fos.close();
        } catch (IOException e) {
            logger.error("NaiveParquetChunkingAlgorithm Chunking error");
        }
        logger.info("total chunk size: " + humanReadable(total_chunk_size));
	}
}
