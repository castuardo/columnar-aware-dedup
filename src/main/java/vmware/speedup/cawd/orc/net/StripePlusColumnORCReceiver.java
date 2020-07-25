package vmware.speedup.cawd.orc.net;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcProto;

import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.Comparators;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.common.TransferStats.TransferStatValue;
import vmware.speedup.cawd.net.SpeedupReceiver;
import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkStore;
import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm;
import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk;
import vmware.speedup.cawd.orc.dedup.ColumnBasedORCChunkingAlgorithm.ColumnBasedORCFileChunk.ChunkType;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkStore;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk;

public class StripePlusColumnORCReceiver extends SpeedupReceiver {

	private static final Logger logger = LogManager.getLogger(StripePlusColumnORCReceiver.class);
	
	private long totalBytesReceived = 0;
	private DataInputStream is = null;
	private StripePlusColumnORCChunkStore footerStore = new StripePlusColumnORCChunkStore();
	private StripePlusColumnORCChunkStore stripeStore = new StripePlusColumnORCChunkStore();
	private StripePlusColumnORCChunkStore columnStore = new StripePlusColumnORCChunkStore();
	private StripePlusColumnORCChunkingAlgorithm algorithm = new StripePlusColumnORCChunkingAlgorithm();

	private StripePlusColumnORCFileChunk.ChunkType readNextType(DataInputStream is) throws IOException {
		int nextOrdinal = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
		return StripePlusColumnORCFileChunk.fromOrdinal(nextOrdinal);
	}
	
	// receive footer
	private TransferStats receiveFooter(
			String fileName, DataInputStream is, OutputStream os, FileOutputStream fos) throws IOException, NoSuchAlgorithmException {
		TransferStats stats = new TransferStats(fileName);
		int transferBytes = 0;
		// we initially received a signature, since here we already consumed the type
		byte[] footerSignature = BytesUtil.readNextBytes(is, StripePlusColumnORCChunkingAlgorithm.SHA1_SIZE);
		int ack = 0;
		StripePlusColumnORCFileChunk found = null;
		// do we have this signature somewhere?
		if((found = footerStore.findChunkBySignature(footerSignature)) != null) {
			totalBytesReceived += found.getContent().length; 
			// and here we write the content too
			fos.write(found.getContent());
			ack = 1;
		}
		else {
			ack = -1;
		}
		// send back an ack...
		ackDataStream(ack, os);
		transferBytes += Integer.BYTES;
		// if we did not have it, then there is one more message for us
		if(ack < 0) {
			// we dont have it, so we will receive the whole footer here
			int size = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
			logger.debug("Dont have this footer, receiving {} bytes", size);
			byte [] footer = BytesUtil.readNextBytes(is, size);
			// hash it and keep it in the footer store...
			footerStore.addChunks(footer, algorithm);
			totalBytesReceived += footer.length; 
			// and write the content
			fos.write(footer);
		}
		
		// done here
		stats.getStats().add(
				new TransferStatValue(TransferStatValue.Type.TransferBytes, transferBytes, TransferStatValue.Unit.Bytes));
		return stats;
	}
	
	private TransferStats receiveStripe(String fileName, DataInputStream is, OutputStream os, FileOutputStream fos, boolean firstStripe) throws IOException, NoSuchAlgorithmException {
		TransferStats stats = new TransferStats(fileName);
		int transferBytes = 0;
		// the transfer initiates with a stripe signature
		byte [] dataSignature = BytesUtil.readNextBytes(is, StripePlusColumnORCChunkingAlgorithm.SHA1_SIZE);
		// its on the store?
		int ack = 0;
		StripePlusColumnORCFileChunk found = null;
		boolean wholeStripeMatch = false;
		// do we have this signature somewhere?
		if((found = stripeStore.findChunkBySignature(dataSignature)) != null) {
			// also, here i am supposed to have the index and the footer linked
			// the sbchunks are supposed to be in order...
			List<StripePlusColumnORCFileChunk> pieces = found.getSubchunks();
			for(StripePlusColumnORCFileChunk piece : pieces) {
				if(piece.getType().equals(StripePlusColumnORCFileChunk.ChunkType.StripeIndex)) {
					fos.write(piece.getContent());
					totalBytesReceived += piece.getContent().length; 
				}
				else if (piece.getType().equals(StripePlusColumnORCFileChunk.ChunkType.StripeFooter)){
					fos.write(piece.getContent());
					// and here we write the content too
					fos.write(found.getContent());
					totalBytesReceived += piece.getContent().length + found.getContent().length; 
				}
			}
			logger.debug("Whole stripe match!");
			wholeStripeMatch = true;
			ack = 1;
		}
		else {
			logger.debug("Could not match stripe");
			ack = -1;
		}
		// write the ack...
		ackDataStream(ack, os);
		transferBytes += Integer.BYTES;
		// we were not able to match the stripe data, so we will need to receive it.
		if(ack < 0) {
			// we will try to receive columns first. So here, we have an int
			int numColumns = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
			// we will keep the failed indexes here
			Queue<Integer> failureIndexes = new LinkedList<Integer>();
			byte[][] partialStripeData = new byte[numColumns][];
			// lets go!
			StripePlusColumnORCFileChunk.ChunkType nextType = null;
			logger.debug("Stripe miss, receiving {} columns", numColumns);
			for(int i = 0; i < numColumns; ++i) {
				ack = 0;
				// we are being sent one of two things:
				// we are being sent a direct match
				nextType = readNextType(is);
				logger.debug("Next column: {}", nextType.name());
				if(nextType.equals(StripePlusColumnORCFileChunk.ChunkType.ColumnQuery)) {
					logger.debug("Querying column {}", i);
					// we are being queried, here comes a hash
					byte[] columnHash = BytesUtil.readNextBytes(is, StripePlusColumnORCChunkingAlgorithm.SHA1_SIZE);
					// do we have it in the column store?
					if((found = columnStore.findChunkBySignature(columnHash)) != null) {
						logger.debug("Found in the columns store");
						// we have it in the column store, we can safely accumulate. Notice that we will need the footer 
						// and the index too, so we cannot write to the file yet...
						partialStripeData[i] = found.getContent();
						totalBytesReceived += found.getContent().length;
						// and the ack, which is the number of predictions
						ack = 1;
					}
					else {
						// we dont have this column, so its a failure
						failureIndexes.offer(i);
						ack = -1;
						logger.debug("Column not found, try the next one...");
					}
					// send back an ack indicating we found the column...
					ackDataStream(ack, os);
					transferBytes += Integer.BYTES;
				}
				else if(nextType.equals(StripePlusColumnORCFileChunk.ChunkType.SmallColumn)) {
					// this column is too small to sign, so we just mark it as failure...
					failureIndexes.offer(i);
					ack = -1;
					logger.debug("Dont care about small columns");
				}
				else {
					logger.info("Received unknown type of column request...");
					throw new IOException("Unknown column request!");
				}
				// done with this column...
			}
			// there was no match, so we are going to receive the whole stripe here
			if(failureIndexes.size() == numColumns) {
				// the whole stripe is <size><index><size><data><size><footer>
				logger.debug("No column matched, receiving whole stripe");
				byte [] index = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				byte [] data = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				byte [] footer = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				// now, i need to store it in a good way
				StripePlusColumnORCFileChunk dataChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeData, data);
				StripePlusColumnORCFileChunk indexChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeIndex, index);
				StripePlusColumnORCFileChunk footerChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeFooter, footer);
				// set the links
				dataChunk.getSubchunks().add(indexChunk);
				dataChunk.getSubchunks().add(footerChunk);
				// and store
				stripeStore.addChunksWithLinks(dataChunk, algorithm);
				// we will also populate the column store here. We need to have an idea of the offsets, and we can do that reading the stripe footer...
				OrcProto.StripeFooter stripeFooter = OrcProto.StripeFooter.parseFrom(footer);
				// the index here is relative to the stripe data, which is zero
				int currentOffset = 0;
				List<StripePlusColumnORCFileChunk> columns = algorithm.getStripeColumnChunks(stripeFooter, currentOffset);
				for(StripePlusColumnORCFileChunk column : columns) {
					byte[] columnChunk = new byte[(int)column.getSize()];
					// copy the content
					System.arraycopy(data, currentOffset, columnChunk, 0, columnChunk.length);
					// and add it to the store
					columnStore.addChunks(columnChunk, algorithm);
					currentOffset += columnChunk.length;
				}
				logger.debug("Populated {} columns", columns.size());
				totalBytesReceived += index.length + data.length + footer.length;
				// and we are done here...
				logger.debug("Received index={}, data={}, footer={}", index.length, data.length, footer.length);
			}
			else if(failureIndexes.size() > 0){
				// we have partial matches, so we need to get the data
				while(failureIndexes.size() > 0) {
					int nextIndex = failureIndexes.poll();
					int nextSize = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
					byte[] nextColumn = BytesUtil.readNextBytes(is, nextSize);
					partialStripeData[nextIndex] = nextColumn;
					totalBytesReceived += nextColumn.length;
					// btw, we received the column, so we can add it to chunk store...
					columnStore.addChunks(nextColumn, algorithm);
				}
				// since we are here, we need the stripe index and stripe footer too...
				byte[] index = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				byte[] footer = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				// if this is the first stripe, we need to write "ORC"
				if(firstStripe) {
					fos.write(new String("ORC").getBytes());
				}
				// and now write the index
				fos.write(index);
				int dataSize = 0;
				for(int j = 0; j < partialStripeData.length; ++j) {
					fos.write(partialStripeData[j]);
					dataSize += partialStripeData[j].length;
				}
				fos.write(footer);
				totalBytesReceived += index.length + footer.length;
				// we are not done yet, we can add a new stripe here
				byte [] stripeData = new byte[dataSize];
				int dataOffset = 0;
				for(int j = 0; j < partialStripeData.length; ++j) {
					System.arraycopy(partialStripeData[j], 0, stripeData, dataOffset, partialStripeData[j].length);
					dataOffset += partialStripeData[j].length;
				}
				// now store
				StripePlusColumnORCFileChunk dataChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeData, stripeData);
				StripePlusColumnORCFileChunk indexChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeIndex, index);
				StripePlusColumnORCFileChunk footerChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeFooter, footer);
				// set the links
				dataChunk.getSubchunks().add(indexChunk);
				dataChunk.getSubchunks().add(footerChunk);
				// and store
				stripeStore.addChunksWithLinks(dataChunk, algorithm);
				// and done...
			}
			// this is a strange corner case. I dont have the stripe in the store for some reason, but i have all the columns...
			else if(failureIndexes.size() == 0 && !wholeStripeMatch) {
				logger.info("Dont have the stripe, but have all columns...");
				// since we are here, we need the stripe index and stripe footer too...
				byte[] index = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				byte[] footer = BytesUtil.readNextBytes(is, BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES)));
				// if this is the first stripe, we need to write "ORC"
				if(firstStripe) {
					fos.write(new String("ORC").getBytes());
				}
				// and now write the index
				fos.write(index);
				int dataSize = 0;
				for(int j = 0; j < partialStripeData.length; ++j) {
					fos.write(partialStripeData[j]);
					dataSize += partialStripeData.length;
				}
				fos.write(footer);
				totalBytesReceived += index.length + footer.length;
				// we are not done yet, we can add a new stripe here
				byte [] stripeData = new byte[dataSize];
				int dataOffset = 0;
				for(int j = 0; j < partialStripeData.length; ++j) {
					System.arraycopy(partialStripeData[j], 0, stripeData, dataOffset, partialStripeData[j].length);
					dataOffset += partialStripeData[j].length;
				}
				// now store
				StripePlusColumnORCFileChunk dataChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeData, stripeData);
				StripePlusColumnORCFileChunk indexChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeIndex, index);
				StripePlusColumnORCFileChunk footerChunk = new StripePlusColumnORCFileChunk(StripePlusColumnORCFileChunk.ChunkType.StripeFooter, footer);
				// set the links
				dataChunk.getSubchunks().add(indexChunk);
				dataChunk.getSubchunks().add(footerChunk);
				// and store
				stripeStore.addChunksWithLinks(dataChunk, algorithm);
			}
		}
		// done here
		stats.getStats().add(
				new TransferStatValue(TransferStatValue.Type.TransferBytes, transferBytes, TransferStatValue.Unit.Bytes));
		return stats;
	}

	
	@Override
	public TransferStats receiveFile(String destinationFolder, InputStream is, OutputStream os) throws IOException {
		FileOutputStream fos = null;
		try {
			if(this.is == null) {
				if(is instanceof DataInputStream) {
					this.is = (DataInputStream)is;
				}
				else {
					throw new IOException("InputStream is expected to be DataInputStream");
				}
			}
			TransferMeta meta = initiateDataStreaming(this.is);
			if(meta != null) {
				boolean firstStripe = true;
				logger.debug("Receiving {} of size {}", meta.getName(), meta.getSize());
				totalBytesReceived = 0;
				String fileName = destinationFolder + File.separator + meta.getName();
				fos = new FileOutputStream(fileName);
				TransferStats all = new TransferStats(fileName);
				all.getStats().add(new TransferStatValue(
						TransferStatValue.Type.ExtraTransferBytes, meta.getTotalLength() + Integer.BYTES , TransferStatValue.Unit.Bytes));
				while(totalBytesReceived < meta.getSize()) {
					TransferStats stats = null;
					StripePlusColumnORCFileChunk.ChunkType nextChunkType = readNextType(this.is);
					switch(nextChunkType) {
						case Stripe:
							stats = receiveStripe(fileName, this.is, os, fos, firstStripe);
							firstStripe = false;
							break;
						case FileFooter:
							stats = receiveFooter(fileName, this.is, os, fos);
							break;
						default:
							logger.error("Receiving wrong type={}, exiting...", nextChunkType);
							throw new IOException("Error when checking next type of data");
					}
					logger.debug("{} bytes remaining...", meta.getSize() - totalBytesReceived);
					// append
					if(stats != null) {
						all.appendStats(stats);
					}
				}
				// flush the file
				fos.flush();
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
		catch(NoSuchAlgorithmException e) {
			logger.error("Transfer failed for bad algorithm...", e);
			ackDataStream(-1, os);
			throw new IOException(e);
		}
		finally {
			if(fos != null) {
				fos.flush();
				fos.close();
			}
		}
	}
	
}
