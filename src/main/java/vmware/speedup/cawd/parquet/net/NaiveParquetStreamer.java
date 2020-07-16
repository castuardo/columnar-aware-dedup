package vmware.speedup.cawd.parquet.net;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.common.TransferStats.TransferStatValue;
import vmware.speedup.cawd.net.SpeedupStreamer;
import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm;
import vmware.speedup.cawd.parquet.dedup.NaiveParquetChunkingAlgorithm.ParquetFileChunk;

public class NaiveParquetStreamer extends SpeedupStreamer {

	private static final Logger logger = LogManager.getLogger(NaiveParquetStreamer.class);
	
	private NaiveParquetChunkingAlgorithm algorithm = new NaiveParquetChunkingAlgorithm();
	
	// Regular chunk:
	// <type-int><size-long><data>	
	private TransferStats handleRegularChunk(String fileName, ParquetFileChunk regular, OutputStream os, FileInputStream fis) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		// this is simple, here we just send both the size and the data in a single piece...
		byte[] buffer = new byte[Integer.BYTES + Long.BYTES + (int)regular.getSize()];
		// get from the file...
		int read = fis.read(buffer, Integer.BYTES + Long.BYTES, (int)regular.getSize());
		// copy the type into 
		System.arraycopy(BytesUtil.intToBytes(ParquetFileChunk.toOrdinal(regular.getType())), 0, buffer, 0, Integer.BYTES);
		// copy size into send buffer
		System.arraycopy(BytesUtil.longToBytes(read), 0, buffer, Integer.BYTES, Long.BYTES);
		// and send
		os.write(buffer);
		os.flush();
		stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.TransferBytes, read , TransferStatValue.Unit.Bytes));
		stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.ExtraTransferBytes, Long.BYTES + Integer.BYTES , TransferStatValue.Unit.Bytes));
		return stats;
	}
	
	// Special chunk intial request:
	// <type-int><hashSize-int><hash>
	// Special chunk reply:
		// 0: dont have it
		// 1: thanks!
	private TransferStats handleSpecialChunk(String fileName, ParquetFileChunk special, InputStream is, OutputStream os, FileInputStream fis) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		int sentBytes = 0;
		int overheadBytes = 0;
		try {
			// read the content first
			byte[] content = new byte[(int)special.getSize()];
			// read it from file
			fis.read(content);
			// hash it...
			byte[] signature = algorithm.naiveSHA1(content);
			// and we need to send it
			byte[] buffer = new byte[Integer.BYTES + Integer.BYTES + signature.length];
			System.arraycopy(BytesUtil.intToBytes(ParquetFileChunk.toOrdinal(special.getType())), 0, buffer, 0, Integer.BYTES);
			System.arraycopy(BytesUtil.intToBytes(signature.length), 0, buffer, Integer.BYTES, Integer.BYTES);
			System.arraycopy(signature, 0, buffer, Integer.BYTES + Integer.BYTES, signature.length);
			// and send it...
			os.write(buffer);
			os.flush();
			overheadBytes += buffer.length;
			// now, we need to wait for the answer
			byte[] reply = new byte[Integer.BYTES];
			((DataInputStream)is).readFully(reply, 0, Integer.BYTES);
			int ack = BytesUtil.bytesToInt(reply);
			logger.debug("Received {} as ack", ack);
			if(ack < 0) {
				// dont have it, i have to send it again...
				byte[] contentMessage = new byte[(int)special.getSize() + Integer.BYTES];
				System.arraycopy(BytesUtil.intToBytes(content.length), 0, contentMessage, 0, Integer.BYTES);
				System.arraycopy(content, 0, contentMessage, Integer.BYTES, content.length);
				// send...
				os.write(contentMessage);
				os.flush();
				sentBytes += content.length;
				overheadBytes += Integer.BYTES;
			}
			else {
				stats.getStats().add(new TransferStatValue(
						TransferStatValue.Type.DedupBytes, content.length , TransferStatValue.Unit.Bytes));
			}
			stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.TransferBytes, sentBytes , TransferStatValue.Unit.Bytes));
			stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.ExtraTransferBytes, overheadBytes , TransferStatValue.Unit.Bytes));
		}
		catch(Exception e) {
			throw new IOException(e);
		}
		return stats;
	}
	
	
	@Override
	public TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		FileInputStream fis = new FileInputStream(fileName);
		try {
            stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.FileBytes, fis.available(), TransferStatValue.Unit.Bytes));
			
            logger.info("Starting file transfer for {}", fileName);
			TransferStats nn = initiateTransfer(fileName, os);
			stats.appendStats(nn);
			long startTime = System.currentTimeMillis();
			List<ParquetFileChunk> chunks = algorithm.eagerChunking(fileName);
			long parquetParsingOverhead = System.currentTimeMillis() - startTime;
			stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.ParsingOverhead, parquetParsingOverhead , TransferStatValue.Unit.Milliseconds));
			logger.debug("{}", Arrays.toString(chunks.toArray()));
			// now do the hustle...
			for(ParquetFileChunk chunk : chunks) {
				TransferStats partial = null;
				switch(chunk.getType()) {
					case DataPageV1:
                    case DataPageV2:
                    case ParquetFooter:
                        logger.debug("Sending special chunk");
						partial = handleSpecialChunk(fileName, chunk, is, os, fis);
						break;
					default:
						logger.debug("Sending regular chunk");
						partial = handleRegularChunk(fileName, chunk, os, fis);
                }
				// append transfer stats
				stats.appendStats(partial);
			}
			// check if we have some ack here
			TransferStatus status = waitForAck(is);
			logger.debug("TransferStatus={}", status.name());
			if(status == TransferStatus.ERROR) {
				logger.error("Received error signal from server...");
				throw new IOException("Transfer failed with error from server!");
			}
			else if(status == TransferStatus.SUCCESS) {
				stats.getStats().add(new TransferStatValue(
						TransferStatValue.Type.TransferTime, System.currentTimeMillis() - startTime , TransferStatValue.Unit.Milliseconds));
			}
			// return aggregated stats...
			return TransferStats.aggregate(stats);	
		}
		finally {
			if(fis != null) fis.close();
		}
	}
	
	
	

}
