package vmware.speedup.cawd.orc.net;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.ColumnTypes.ORCColumnType;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.common.TransferStats.TransferStatValue;
import vmware.speedup.cawd.net.SpeedupStreamer;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk;
import vmware.speedup.cawd.orc.dedup.StripePlusColumnORCChunkingAlgorithm.StripePlusColumnORCFileChunk.ChunkType;

public class StripePlusColumnORCStreamer extends SpeedupStreamer {

	private static final Logger logger = LogManager.getLogger(StripePlusColumnORCStreamer.class);
	
	private StripePlusColumnORCChunkingAlgorithm algorithm = new StripePlusColumnORCChunkingAlgorithm();
	private DataInputStream is = null;
	
	private TransferStats sendFooter(
			String fileName, StripePlusColumnORCFileChunk footer, DataInputStream is, OutputStream os, FileInputStream fis) throws IOException, NoSuchAlgorithmException {
		logger.debug("Sending footer of size={} bytes", footer.getSize());
		TransferStats stats = new TransferStats(fileName);
		stats.getStats().add(new TransferStatValue(TransferStatValue.Type.FooterSize, footer.getSize(), TransferStatValue.Unit.Bytes));
		// hold data
		byte[] footerData = new byte[(int)footer.getSize()];
		int footerOrdinal = StripePlusColumnORCFileChunk.toOrdinal(StripePlusColumnORCFileChunk.ChunkType.FileFooter);
		// stats
		int totalBytesSent = 0;
		// now read
		fis.read(footerData, 0, footerData.length);
		byte [] footerHash = algorithm.naiveSHA1(footerData);
		// and send it with type, to signal what we are sending here...
		byte [] footerBuffer = new byte[Integer.BYTES + StripePlusColumnORCChunkingAlgorithm.SHA1_SIZE];
		System.arraycopy(BytesUtil.intToBytes(footerOrdinal), 0, footerBuffer, 0, Integer.BYTES);
		System.arraycopy(footerHash, 0, footerBuffer, Integer.BYTES, footerHash.length);
		// and send it...
		os.write(footerBuffer);
		totalBytesSent += footerBuffer.length;
		os.flush();
		// lets see if we had a match
		int ack = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
		if(ack > 0) {
			// footer was matched, so we have a hit!
			stats.getStats().add(new TransferStatValue(TransferStatValue.Type.FooterHit, 1, TransferStatValue.Unit.Count));
			stats.getStats().add(new TransferStatValue(TransferStatValue.Type.DedupBytes, footerData.length, TransferStatValue.Unit.Bytes));
			logger.debug("Footer hit");
		}
		else {
			// now match, send it back
			byte[] footerPack = new byte[Integer.BYTES + footerData.length];
			// assemble it...
			System.arraycopy(BytesUtil.intToBytes(footerData.length), 0, footerPack, 0, Integer.BYTES);
			System.arraycopy(footerData, 0, footerPack, Integer.BYTES, footerData.length);
			// and just write...
			os.write(footerPack);
			totalBytesSent += footerPack.length;
			os.flush();
			logger.debug("Footer miss, sending {} bytes (footer + size)", footerPack.length);
			// and count the miss
			stats.getStats().add(
					new TransferStatValue(TransferStatValue.Type.FooterMiss, 1, TransferStatValue.Unit.Count));
		}
		stats.getStats().add(new TransferStatValue(TransferStatValue.Type.TransferBytes, totalBytesSent, TransferStatValue.Unit.Bytes));
		// done...
		return stats;
	}
	
	private TransferStats sendStripe(
			String fileName, StripePlusColumnORCFileChunk index, 
			StripePlusColumnORCFileChunk data, StripePlusColumnORCFileChunk footer, 
			DataInputStream is, OutputStream os, FileInputStream fis, boolean firstStripe) throws IOException, NoSuchAlgorithmException {
		logger.debug("Sending stripe, index={} bytes, data={} bytes, footer={} bytes", index.getSize(), data.getSize(), footer.getSize());
		TransferStats stats = new TransferStats(fileName);
		stats.getStats().add(new TransferStatValue(TransferStatValue.Type.StripeSize, index.getSize() + data.getSize() + footer.getSize(), TransferStatValue.Unit.Bytes));
		// here we will negotiate the stripe sending...
		byte[] stripeIndex = new byte[(int)index.getSize()];
		byte[] stripeData = new byte[(int)data.getSize()];
		byte[] stripeFooter = new byte[(int)footer.getSize()];
		int stripeOrdinal = StripePlusColumnORCFileChunk.toOrdinal(StripePlusColumnORCFileChunk.ChunkType.Stripe);
		int colQueryOrdinal = StripePlusColumnORCFileChunk.toOrdinal(StripePlusColumnORCFileChunk.ChunkType.ColumnQuery);
		int smallQueryOrdinal = StripePlusColumnORCFileChunk.toOrdinal(StripePlusColumnORCFileChunk.ChunkType.SmallColumn);
		boolean wholeStripeMatch = false;
		// load the stripe
		if(firstStripe) {
			// we need to load the first 3 bytes if this is the first stripe
			byte [] txt = new byte[3];
			fis.read(txt, 0, 3);
			logger.debug("Read text from first stripe={}", new String(txt));
		}
		fis.read(stripeIndex, 0, stripeIndex.length);
		fis.read(stripeData, 0, stripeData.length);
		fis.read(stripeFooter, 0, stripeFooter.length);
		// stats
		int totalBytesSent = 0;
		// hash the data
		byte [] dataHash = algorithm.naiveSHA1(stripeData);
		// and send it with type, to signal what we are sending here...
		byte [] dataBuffer = new byte[Integer.BYTES + StripePlusColumnORCChunkingAlgorithm.SHA1_SIZE];
		System.arraycopy(BytesUtil.intToBytes(stripeOrdinal), 0, dataBuffer, 0, Integer.BYTES);
		System.arraycopy(dataHash, 0, dataBuffer, Integer.BYTES, dataHash.length);
		// and send it...
		os.write(dataBuffer);
		totalBytesSent += dataBuffer.length;
		os.flush();
		// now, lets wait for the related stripe ack
		int ack = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
		if(ack > 0) {
			// they matched the whole stripe, so there is a stripe hit. The other side has to 
			// keep the index and the footer linked there
			stats.getStats().add(
					new TransferStatValue(TransferStatValue.Type.StripeHit, 1, TransferStatValue.Unit.Count));
			stats.getStats().add(new TransferStatValue(TransferStatValue.Type.DedupBytes, stripeData.length + stripeIndex.length + stripeFooter.length, TransferStatValue.Unit.Bytes));
			logger.debug("Stripe hit");
			wholeStripeMatch = true;
		}
		else {
			// here we have a fallback. We need to try to match columns. Here, we will send the hash of a colummn.
			// if we have a hit, we will receive back the hash of the next column. If they match, we will just ack
			// and keep saving.
			List<StripePlusColumnORCFileChunk> columns = data.getSubchunks();
			// i will keep the failed indexes here
			Queue<Integer> failureIndexes = new LinkedList<Integer>();
			// this offset refers to the columns content
			int currentOffset = 0;
			// this is to count the columns
			int currentColumn = 0;
			List<byte[]> chunkedColumns = new ArrayList<byte[]>();
			// i will send an int to see how many columns we will receive
			os.write(BytesUtil.intToBytes(columns.size()));
			totalBytesSent += Integer.BYTES;
			os.flush();				
			if(columns.size() > 1) {
				logger.debug("Stripe miss, going to send {} columns", columns.size());
				for(StripePlusColumnORCFileChunk column : columns) {
					// send the first hash
					byte[] colBytes = new byte[(int)column.getSize()];
					System.arraycopy(stripeData, currentOffset, colBytes, 0, colBytes.length);
					chunkedColumns.add(colBytes);
					byte[] colPack = null;
					if(colBytes.length > StripePlusColumnORCChunkingAlgorithm.SHA1_SIZE * 2) {
						byte[] colHash = algorithm.naiveSHA1(colBytes);
						// prepare the message
						colPack = new byte[Integer.BYTES + colHash.length];
						// we need to send the column hash, that is of type column query
						System.arraycopy(BytesUtil.intToBytes(colQueryOrdinal), 0, colPack, 0, Integer.BYTES);
						System.arraycopy(colHash, 0, colPack, Integer.BYTES, colHash.length);
						// i dont need to send a size here, since all of these guys are the same size...
						os.write(colPack);
						totalBytesSent += colPack.length;
						os.flush();
						// now, i need to get the reply. This can be negative, zero or positive. When positive or zero, there is 
						// a prediction mechanism sending at most k next column hashes. So be careful here, 0 means match but no
						// linked chunks!
						ack = BytesUtil.bytesToInt(BytesUtil.readNextBytes(is, Integer.BYTES));
						if(ack > 0) {
							// this is a hit, so count it
							logger.debug("column hit {}", currentColumn);
							stats.getStats().add(new TransferStatValue(TransferStatValue.Type.ColumnHit, 1, TransferStatValue.Unit.Count));
							stats.getStats().add(new TransferStatValue(TransferStatValue.Type.DedupBytes, colBytes.length, TransferStatValue.Unit.Bytes));
						}
						else {
							// its negative, we will keep this as failed index
							failureIndexes.offer(currentColumn);
							logger.debug("column miss {}", currentColumn);
							stats.getStats().add(new TransferStatValue(TransferStatValue.Type.ColumnMiss, 1, TransferStatValue.Unit.Count));
							if(column.getDataType() == ORCColumnType.String) {
								stats.getStats().add(new TransferStatValue(TransferStatValue.Type.StringColumnMiss, 1, TransferStatValue.Unit.Count));
								stats.getStats().add(new TransferStatValue(TransferStatValue.Type.StringColumnMissBytes, column.getSize(), TransferStatValue.Unit.Bytes));
							}
						}
					}
					else {
						colPack = BytesUtil.intToBytes(smallQueryOrdinal);
						os.write(colPack);
						totalBytesSent += colPack.length;
						os.flush();
						// this column is too small, so we dont care
						failureIndexes.offer(currentColumn);
						logger.debug("Small column {}, size={}", currentColumn, colBytes.length);
						stats.getStats().add(new TransferStatValue(TransferStatValue.Type.SmallColumn, 1, TransferStatValue.Unit.Count));
					}
					// and update the offset
					currentOffset += colBytes.length;
					// and the current column index
					++currentColumn;
				}
			}
			else {
				os.write(BytesUtil.intToBytes(smallQueryOrdinal));
				totalBytesSent += Integer.BYTES;
				os.flush();
				logger.debug("File has single column, if no stripe match then there is no point trying...");
				// just one column, dont care...
				failureIndexes.offer(0);
			}
			// they dont have that data, so we will just send the whole stripe. This is basically 
			// <size><index><size><data><size><footer>
			if(failureIndexes.size() == columns.size()) {
				// assemble the message
				byte [] wholeStripe = new byte[Integer.BYTES * 3 + stripeIndex.length + stripeData.length + stripeFooter.length];
				currentOffset = 0;
				
				System.arraycopy(BytesUtil.intToBytes(stripeIndex.length), 0, wholeStripe, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeIndex, 0, wholeStripe, currentOffset, stripeIndex.length);
				currentOffset += stripeIndex.length;
				
				System.arraycopy(BytesUtil.intToBytes(stripeData.length), 0, wholeStripe, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeData, 0, wholeStripe, currentOffset, stripeData.length);
				currentOffset += stripeData.length;
				
				System.arraycopy(BytesUtil.intToBytes(stripeFooter.length), 0, wholeStripe, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeFooter, 0, wholeStripe, currentOffset, stripeFooter.length);
				currentOffset += stripeFooter.length;
				
				os.write(wholeStripe);
				totalBytesSent += wholeStripe.length;
				os.flush();
				
				logger.debug("Sent whole stripe with size={}", wholeStripe.length);
				
				stats.getStats().add(new TransferStatValue(TransferStatValue.Type.StripeMiss, 1, TransferStatValue.Unit.Count));
			}
			// we matched some columns but not all...
			else if(failureIndexes.size() > 0) {
				logger.debug("We missed {}% columns, we will send them", String.format("%.3f", 100.0 * (failureIndexes.size() * 1.0 / columns.size() * 1.0)));
				// the other side knows how many columns failed, so we are fine. We need to send the whole individual columns here...
				while(failureIndexes.size() > 0) {
					int nextColumn = failureIndexes.poll();
					byte [] chunkedColumn = chunkedColumns.get(nextColumn);
					logger.debug("Getting column {}, size={}", nextColumn, chunkedColumn.length);
					byte [] colPack = new byte[Integer.BYTES + chunkedColumn.length];
					System.arraycopy(BytesUtil.intToBytes(chunkedColumn.length), 0, colPack, 0, Integer.BYTES);
					System.arraycopy(chunkedColumn, 0, colPack, Integer.BYTES, chunkedColumn.length);
					os.write(colPack);
					totalBytesSent += colPack.length;
					os.flush();
				}
				// also, we need index and footer
				byte [] indexPlusFooter = new byte[Integer.BYTES * 2 + stripeIndex.length + + stripeFooter.length];
				currentOffset = 0;
				
				System.arraycopy(BytesUtil.intToBytes(stripeIndex.length), 0, indexPlusFooter, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeIndex, 0, indexPlusFooter, currentOffset, stripeIndex.length);
				currentOffset += stripeIndex.length;
				
				System.arraycopy(BytesUtil.intToBytes(stripeFooter.length), 0, indexPlusFooter, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeFooter, 0, indexPlusFooter, currentOffset, stripeFooter.length);
				currentOffset += stripeFooter.length;
				
				os.write(indexPlusFooter);
				totalBytesSent += indexPlusFooter.length;
				os.flush();
				// done here...
			}
			// corner case, dont have the stripe but i have all the columns...
			else if(failureIndexes.size() == 0 && !wholeStripeMatch) {
				// also, we need index and footer
				byte [] indexPlusFooter = new byte[Integer.BYTES * 2 + stripeIndex.length + + stripeFooter.length];
				currentOffset = 0;
				
				System.arraycopy(BytesUtil.intToBytes(stripeIndex.length), 0, indexPlusFooter, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeIndex, 0, indexPlusFooter, currentOffset, stripeIndex.length);
				currentOffset += stripeIndex.length;
				
				System.arraycopy(BytesUtil.intToBytes(stripeFooter.length), 0, indexPlusFooter, currentOffset, Integer.BYTES);
				currentOffset += Integer.BYTES;
				System.arraycopy(stripeFooter, 0, indexPlusFooter, currentOffset, stripeFooter.length);
				currentOffset += stripeFooter.length;
				
				os.write(indexPlusFooter);
				totalBytesSent += indexPlusFooter.length;
				os.flush();
			}
			// done...
		}
		stats.getStats().add(new TransferStatValue(TransferStatValue.Type.TransferBytes, totalBytesSent, TransferStatValue.Unit.Bytes));
		return stats;
	}
	
	@Override
	public TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		FileInputStream fis = new FileInputStream(fileName);
		try {
			if(this.is == null) {
				if(is instanceof DataInputStream) {
					this.is = (DataInputStream)is;
				}
				else {
					throw new IOException("InputStream is expected to be DataInputStream");
				}
			}
			logger.info("Starting file transfer for {}", fileName);
			// first stripe
			boolean firstStripe = true;
			TransferStats nn = initiateTransfer(fileName, os);
			stats.appendStats(nn);
			long startTime = System.currentTimeMillis();
			List<StripePlusColumnORCFileChunk> chunks = algorithm.eagerChunking(fileName);
			long orcParsingOverhead = System.currentTimeMillis() - startTime;
			stats.getStats().add(new TransferStatValue(
					TransferStatValue.Type.ParsingOverhead, orcParsingOverhead , TransferStatValue.Unit.Milliseconds));
			logger.debug("{}", Arrays.toString(chunks.toArray()));
			// now do the hustle...
			StripePlusColumnORCFileChunk index = null, data = null, footer = null;
			for(StripePlusColumnORCFileChunk chunk : chunks) {
				TransferStats partial = null;
				// handle the footer here
				if(chunk.getType().equals(ChunkType.FileFooter)) {
					// just handle this one here...
					partial = sendFooter(fileName, chunk, this.is, os, fis);
					// there should never be an iteration after this is done
					index = data = footer = null; 
				}
				// get the index
				if(chunk.getType().equals(ChunkType.StripeIndex)) {
					index = chunk;
				}
				// get the data
				else if (chunk.getType().equals(ChunkType.StripeData)) {
					data = chunk;
				}
				// get the footer
				else if (chunk.getType().equals(ChunkType.StripeFooter)) {
					footer = chunk;
				}
				if(index != null && data != null && footer != null) {
					// i can send the whole stripe
					partial = sendStripe(fileName, index, data, footer, this.is, os, fis, firstStripe);
					index = data = footer = null; 
					firstStripe = false;
				}
				if(partial != null) {
					// append transfer stats
					stats.appendStats(partial);
				}
			}
			// check if we have some ack here
			TransferStatus status = waitForAck(is);
			logger.debug("TransferStatus={}", status.name());
			if(status == TransferStatus.ERROR) {
				logger.error("Received error signal from server...");
				throw new IOException("Transfer failed with error from server!");
			}
			else if(status == TransferStatus.SUCCESS) {
				logger.info("Tranfer done");
				stats.getStats().add(new TransferStatValue(
						TransferStatValue.Type.TransferTime, System.currentTimeMillis() - startTime , TransferStatValue.Unit.Milliseconds));
				stats.getStats().add(new TransferStatValue(
						TransferStatValue.Type.TotalBytes, new File(fileName).length() , TransferStatValue.Unit.Bytes));
			}
			// return aggregated stats...
			return TransferStats.aggregate(stats);	
		}
		catch(NoSuchAlgorithmException e) {
			// something bad
			logger.error("Algorithm not found!", e);
			return null;
		}
		finally {
			if(fis != null) fis.close();
		}
	}

}
