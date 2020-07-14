package vmware.speedup.cawd.net;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.TransferStats;
import vmware.speedup.cawd.common.TransferStats.TransferStatValue;
import vmware.speedup.cawd.dedup.ColumnarChunkStore;

public abstract class SpeedupStreamer {
	
	public abstract TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException;
	
	public static enum TransferStatus {
		ONGOING,
		SUCCESS,
		ERROR
	}
	
	protected TransferStats initiateTransfer(String fileName, OutputStream os) throws IOException {
		TransferStats stats = new TransferStats(fileName);
		// compose...
		File tmp = new File(fileName);
		long fileLength = tmp.length();
		int nameLength = tmp.getName().length();
		byte [] buffer = new byte[Integer.BYTES + nameLength + Long.BYTES];
		System.arraycopy(BytesUtil.intToBytes(nameLength), 0, buffer, 0, Integer.BYTES);
		System.arraycopy(tmp.getName().getBytes(), 0, buffer, Integer.BYTES, tmp.getName().getBytes().length);
		System.arraycopy(BytesUtil.longToBytes(fileLength), 0, buffer, Integer.BYTES + tmp.getName().getBytes().length, Long.BYTES);
		// and send this
		os.write(buffer);
		os.flush();stats.getStats().add(new TransferStatValue(
				TransferStatValue.Type.ExtraTransferBytes, Integer.BYTES + nameLength + Long.BYTES , TransferStatValue.Unit.Bytes));
		return stats;
		
	}
	
	protected TransferStatus waitForAck(InputStream is) throws IOException {
		while(is.available() != Integer.BYTES) {	
			// wait here
			try {
				Thread.sleep(1);
			}
			catch(Exception e) {}
		}
		byte [] ack = new byte[Integer.BYTES];
		is.read(ack, 0, Integer.BYTES);
		int a = BytesUtil.bytesToInt(ack);
		return a > 0? TransferStatus.SUCCESS : (a == 0? TransferStatus.ONGOING : TransferStatus.ERROR);
	}
	
	public static class PlainSpeedupStreamer extends SpeedupStreamer {
		
		private static final Logger logger = LogManager.getLogger(PlainSpeedupStreamer.class);
		
		private int bufferSize = 0;

		public PlainSpeedupStreamer() {
			this.bufferSize = Integer.valueOf(System.getProperty("cawd.streamer.plain.bufferSize", "4096"));
		}
		
		@Override
		public TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException {
			TransferStats stats = new TransferStats(fileName);
			FileInputStream fis = null;
			long extraTransferBytes = 0;
			try {
				logger.debug("Starting file transfer for {}", fileName);
				File targetFile = new File(fileName);
				// we will first send the name of the file (and the length of the name)
				// this will initiate transmission. We also send the file size
				String name = targetFile.getName();
				os.write(BytesUtil.longToBytes(name.length()));
				extraTransferBytes += Long.BYTES;
				os.write(name.getBytes());
				extraTransferBytes += name.getBytes().length;
				os.write(BytesUtil.longToBytes(targetFile.length()));
				extraTransferBytes += Long.BYTES;
				// now, we open the file and start sending it
				fis = new FileInputStream(fileName);
				// just send in little pieces...
				long remaining = (int)targetFile.length();
				byte[] buffer = new byte[bufferSize];
				double bytesSent = 0;
				double startTime = System.currentTimeMillis();
				while(remaining > 0) {
					// get from file...
					int read = 0;
					if(remaining > bufferSize - Long.BYTES) {
						read = fis.read(buffer, Long.BYTES, bufferSize - Long.BYTES);
					}
					else {
						read = fis.read(buffer, Long.BYTES, fis.available());
					}
					// add the size...
					System.arraycopy(BytesUtil.longToBytes(read), 0, buffer, 0, Long.BYTES);
					// now send...
					os.write(buffer, 0, read + Long.BYTES);
					// done...
					bytesSent += read;
					remaining -= read;
					extraTransferBytes += Long.BYTES;
					logger.debug("sent {}, {} bytes remaining...", read, remaining);
				}		
				// flush...
				os.flush();
				// receive ack
				byte[] ack = new byte[Long.BYTES];
				is.read(ack, 0, Long.BYTES);
				extraTransferBytes += Long.BYTES;
				long ok = BytesUtil.bytesToLong(ack);
				logger.debug("Received ack={}", ok);
				if(ok > 0) {
					stats.getStats().add(new TransferStatValue(
							TransferStatValue.Type.TransferBytes, bytesSent, TransferStatValue.Unit.Bytes));
					stats.getStats().add(new TransferStatValue(
							TransferStatValue.Type.ExtraTransferBytes, extraTransferBytes, TransferStatValue.Unit.Bytes));
					stats.getStats().add(new TransferStatValue(
							TransferStatValue.Type.TransferTime, System.currentTimeMillis() - startTime, TransferStatValue.Unit.Milliseconds));
					logger.debug("Done with {}", fileName);
					return stats;
				}
				else {
					throw new IOException("Transfer failed!");
				}
			}
			finally {
				if(fis != null) {
					fis.close();
				}
			}
		}
		
	}
	
}
