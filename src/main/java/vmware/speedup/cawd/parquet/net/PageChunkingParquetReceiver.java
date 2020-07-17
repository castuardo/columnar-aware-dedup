package vmware.speedup.cawd.parquet.net;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.common.TransferStats.TransferStatValue;
import vmware.speedup.cawd.net.SpeedupReceiver;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkStore;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm.ParquetFileChunk;
import vmware.speedup.cawd.parquet.dedup.PageChunkingParquetChunkingAlgorithm.ParquetFileChunk.ChunkType;

public class PageChunkingParquetReceiver extends SpeedupReceiver {

	private static final Logger logger = LogManager.getLogger(PageChunkingParquetReceiver.class);
	
	private long totalBytesReceived = 0;
	private PageChunkingParquetChunkStore chunkStore = new PageChunkingParquetChunkStore();
	private PageChunkingParquetChunkingAlgorithm algorithm = new PageChunkingParquetChunkingAlgorithm();
	
	// here, a chunk looks like <size-long><data>
	private TransferStats handleRegularChunk(String fileName, InputStream is, FileOutputStream fos) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		byte [] sizeBuff = new byte[Long.BYTES];
		// read
		((DataInputStream)is).readFully(sizeBuff, 0, Long.BYTES);
		// convert
		long size = BytesUtil.bytesToLong(sizeBuff);
		// read data...
		byte [] dataBuff = new byte[(int)size];
		((DataInputStream)is).readFully(dataBuff, 0, (int)size);
		// and write
		fos.write(dataBuff);
		stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.TransferBytes, size , TransferStatValue.Unit.Bytes));
		stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.ExtraTransferBytes, Long.BYTES , TransferStatValue.Unit.Bytes));
		totalBytesReceived += size;
		return stats;
	}
	
	// here, a chunk looks like <hashSize-int><hash>
	private TransferStats handleSpecialChunk(String fileName, InputStream is, OutputStream os, FileOutputStream fos) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		byte[] sizeBuff = new byte[Integer.BYTES];
		((DataInputStream)is).readFully(sizeBuff, 0, Integer.BYTES);
		int hashSize = BytesUtil.bytesToInt(sizeBuff);
		// now read the hash
		byte[] hash = new byte[hashSize];
		((DataInputStream)is).readFully(hash, 0, hashSize);
		stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.ExtraTransferBytes, Integer.BYTES , TransferStatValue.Unit.Bytes));
		stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.ExtraTransferBytes, hashSize , TransferStatValue.Unit.Bytes));
		// do we have it?
		ParquetFileChunk chunk = chunkStore.findChunkBySignature(hash);
		if(chunk != null) {
			// ack and acknowledge we handled it...
			ackDataStream(1, os);
			logger.debug("Sending ack...");
			// and lets write it...
			fos.write(chunk.getContent());
			totalBytesReceived += chunk.getContent().length;
			stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.DedupBytes, chunk.getContent().length , TransferStatValue.Unit.Bytes));
		}
		// nope, we dont, we need to send a request back
		else {
			ackDataStream(-1, os);
			logger.debug("Fallback, send content...");
			// and get the content, this is an int and the content
			((DataInputStream)is).readFully(sizeBuff, 0, Integer.BYTES);
			int size = BytesUtil.bytesToInt(sizeBuff);
			byte[] content = new byte[size];
			((DataInputStream)is).readFully(content, 0, size);
			// and write it...
			fos.write(content);
			// and save it...
			try {
				List<ParquetFileChunk> chunks = chunkStore.addChunks(content, algorithm);
				logger.debug("Added {} chunks to chunk store", chunks.size());
			}
			catch(NoSuchAlgorithmException e) {
				logger.error("Algorithm was not found...", e);
			}
			totalBytesReceived += size;
			stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.TransferBytes, content.length , TransferStatValue.Unit.Bytes));
		}
        stats.getStats().add(new TransferStatValue(
                TransferStatValue.Type.ExtraTransferBytes, Integer.BYTES , TransferStatValue.Unit.Bytes));
		return stats;
		
		
	}

	private ChunkType readNextType(InputStream is) throws IOException {
		byte[] nextTypeOrdinal = new byte[Integer.BYTES];
		((DataInputStream)is).readFully(nextTypeOrdinal, 0, Integer.BYTES);
		return ParquetFileChunk.fromOrdinal(BytesUtil.bytesToInt(nextTypeOrdinal));
	}
	
	@Override
	public TransferStats receiveFile(String destinationFolder, InputStream is, OutputStream os) throws IOException {
		FileOutputStream fos = null;
		try {
			// initiate transfer here
			TransferMeta meta = initiateDataStreaming(is);
			if(meta != null) {
				logger.debug("Receiving {} of size {}", meta.getName(), meta.getSize());
				totalBytesReceived = 0;
				String fileName = destinationFolder + File.separator + meta.getName();
				fos = new FileOutputStream(fileName);
				TransferStats all = new TransferStats(fileName);
				all.getStats().add(new TransferStatValue(
						TransferStatValue.Type.ExtraTransferBytes, meta.getTotalLength() + Integer.BYTES , TransferStatValue.Unit.Bytes));
				while(totalBytesReceived < meta.getSize()) {
					TransferStats stats = null;
					ChunkType nextChunkType = readNextType(is);
                    all.getStats().add(new TransferStatValue(
                        TransferStatValue.Type.ExtraTransferBytes, Integer.BYTES , TransferStatValue.Unit.Bytes));
                    switch(nextChunkType) {
                        case DictPage:
                        case DataPageV1:
                        case DataPageV2:
                        case ParquetFooter:
                            logger.debug("Receiving special chunk...");
							stats = handleSpecialChunk(fileName, is, os, fos);
							break;
						default: 
							logger.debug("Receiving regular chunk...");
							stats = handleRegularChunk(fileName, is, fos);
					}
					// append
					all.appendStats(stats);
				}
				// ack
				ackDataStream(1, os);
				// done
				return TransferStats.aggregate(all);
			}
			// transfer terminated
			logger.info("Terminating transfers...");
			return null;
		}
		catch(IOException e) {
			logger.error("Transfer failed!", e);
			ackDataStream(-1, os);
			throw e;
		}
		finally {
			if(fos != null) {
				fos.flush();
				fos.close();
			}
		}
	}

}
