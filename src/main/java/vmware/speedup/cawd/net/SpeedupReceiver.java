package vmware.speedup.cawd.net;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.dedup.ColumnarChunkStore;
import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.common.TransferStats.TransferStatValue;

public abstract class SpeedupReceiver {

	protected ColumnarChunkStore chunkStore = null;
	
	public abstract TransferStats receiveFile(String destinationFolder, InputStream is, OutputStream os) throws IOException;
	
	public static class TransferMeta {
		
		private String name = null;
		private long size = 0;
		private long totalLength = 0;
		
		public String getName() {
			return name;
		}
		
		public long getSize() {
			return size;
		}

		public long getTotalLength() {
			return totalLength;
		}
		
	}
	
	// here, we read an int, a name (of int bytes) and a long
	protected TransferMeta initiateDataStreaming(InputStream is) throws IOException {
		TransferMeta meta = new TransferMeta();
		// read the size
		byte[] nameSize = new byte[Integer.BYTES];
		is.read(nameSize, 0, Integer.BYTES);
		int ns = BytesUtil.bytesToInt(nameSize);
		if(ns > 0) {
			// read the name
			byte[] name = new byte[ns];
			is.read(name, 0, ns);
			meta.name = new String(name);
			meta.totalLength += ns;
			// and the size of the file
			byte[] fileSize = new byte[Long.BYTES];
			is.read(fileSize, 0, Long.BYTES);
			meta.size = BytesUtil.bytesToLong(fileSize);
			meta.totalLength += Long.BYTES;
			return meta;
		}
		else {
			return null;
		}
	}
	
	protected void ackDataStream(int ack, OutputStream os) throws IOException {
		os.write(BytesUtil.intToBytes(ack));
		os.flush();
	}
	
	public static class PlainSpeedupReceiver extends SpeedupReceiver {

		private static final Logger logger = LogManager.getLogger(SpeedupReceiver.class);
		
		// looks for a long in the stream. If its positive then its the size 
		// of the incomming file name
		private long initiateTransfer(InputStream is) throws IOException {
			logger.debug("Receiving incomming request, {} bytes available", is.available());
			// first, size and name
			byte[] sizeBuffer = new byte[Long.BYTES];
			is.read(sizeBuffer, 0, Long.BYTES);
			long size = BytesUtil.bytesToLong(sizeBuffer);
			return size;
		}
		
		@Override
		public TransferStats receiveFile(String destinationFolder, InputStream is, OutputStream os) throws IOException {
			long size = initiateTransfer(is);
			if(size > 0) {
				String fileName = null;
				FileOutputStream fos = null;
				long extraTransferBytes = Long.BYTES;
				long overhead = 0;
				try {
					byte[] sizeBuffer = new byte[Long.BYTES];
					byte [] name = new byte[(int)size];
					is.read(name, 0, (int)size);
					extraTransferBytes += name.length;
					fileName = new String(name);
					logger.debug("Incoming name {}", fileName);
					is.read(sizeBuffer, 0, Long.BYTES);
					extraTransferBytes += Long.BYTES;
					long fileSize = BytesUtil.bytesToLong(sizeBuffer);
					logger.info("Incomming file: {} of size {}", fileName, fileSize);
					// now, read the whole file
					long received = 0;
					// now, lets write
					String newFileName = new File(destinationFolder).getAbsolutePath() + File.separator + fileName;
					fos = new FileOutputStream(newFileName);
					TransferStats stats = new TransferStats(newFileName);
					long startTime = System.currentTimeMillis();
					while(received < fileSize) {
						// read the chunk size
						is.read(sizeBuffer, 0, Long.BYTES);
						extraTransferBytes += Long.BYTES;
						overhead += Long.BYTES;
						long chunkSize = BytesUtil.bytesToLong(sizeBuffer);
						if(chunkSize > 0) {
							byte [] chunk = new byte[(int)chunkSize];
							is.read(chunk, 0, chunk.length);
							// write it back
							fos.write(chunk, 0, chunk.length);
							received += chunkSize;
							logger.debug("Received {} bytes, {} remaining...", chunkSize, fileSize - received);
						}
						else {
							// done receiving
							logger.info("Terminated receiving, chunk size was negative...");
							break;
						}
					}
					// done..
					fos.flush();
					// send ack
					os.write(BytesUtil.longToBytes(1L));
					os.flush();
					// done
					logger.info("Done with file={}, received={} bytes...", fileName, received);
					stats.getStats().add(new TransferStatValue(
							TransferStatValue.Type.TransferBytes, received + overhead, TransferStatValue.Unit.Bytes));
					stats.getStats().add(new TransferStatValue(
							TransferStatValue.Type.ExtraTransferBytes, extraTransferBytes, TransferStatValue.Unit.Bytes));
					stats.getStats().add(new TransferStatValue(
							TransferStatValue.Type.TransferTime, System.currentTimeMillis() - startTime, TransferStatValue.Unit.Milliseconds));
					return stats;
				}
				catch(IOException e) {
					logger.error("Error while receiving file", e);
					os.write(BytesUtil.longToBytes(-1L));
					os.flush();
					throw e;
				}
				finally {
					if(fos != null) fos.close();
				}
			}
			else {
				logger.info("Received termination signal...");
				return null;
			}
		}
		
	}
	
}
