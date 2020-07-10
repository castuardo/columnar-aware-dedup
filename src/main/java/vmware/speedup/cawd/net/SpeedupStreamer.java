package vmware.speedup.cawd.net;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.dedup.ColumnarChunkStore;

public abstract class SpeedupStreamer {

	protected ColumnarChunkStore chunkStore = null;
	
	public SpeedupStreamer(ColumnarChunkStore chunkStore) {
		this.chunkStore = chunkStore;
	}
	
	public abstract TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException;
	
	public static class TransferStats {
		
		private String filePath = null;
		private List<TransferStatValue> stats = null;
		
		public TransferStats(String filePath) {
			this.filePath = filePath;
			this.stats = new ArrayList<TransferStatValue>();
		}

		public String getFilePath() {
			return filePath;
		}

		public List<TransferStatValue> getStats() {
			return stats;
		}
		
		@Override
		public String toString() {
			return new StringBuilder().append("file=") 
									  .append(filePath)
									  .append(", stats=")
									  .append(Arrays.toString(stats.toArray()))
									  .toString();
		}
	}
	
	public static class TransferStatValue {
		
		public enum Type {
			TransferBytes,
			TransferTime
		}
		
		public enum Unit {
			Bytes,
			Milliseconds
		};
		
		private Type type = null;
		private double value = 0.0;
		private Unit unit = null;
		
		public TransferStatValue(Type type, double value, Unit unit) {
			this.type = type;
			this.value = value;
			this.unit = unit;
		}
		
		@Override
		public String toString() {
			return new StringBuilder().append(type.name())
									  .append("=")
									  .append(value)
									  .append(" ")
									  .append(unit.name())
									  .toString();
		}
		
	}
	
	public static class PlainSpeedupStreamer extends SpeedupStreamer {

		private int bufferSize = 0;

		public PlainSpeedupStreamer(int bufferSize) {
			super(null);
			this.bufferSize = bufferSize;
		}
		
		@Override
		public TransferStats transferFile(String fileName, InputStream is, OutputStream os) throws IOException {
			TransferStats stats = new TransferStats(fileName);
			FileInputStream fis = null;
			try {
				// open the file
				fis = new FileInputStream(fileName);
				// just send in little pieces...
				int remaining = (int)new File(fileName).length();
				int dataSize =  bufferSize - Long.BYTES;
				byte[] buffer = new byte[bufferSize];
				double bytesSent = 0;
				double startTime = System.currentTimeMillis();
				while(remaining > 0) {
					// get from file...
					int read = fis.read(buffer, Long.BYTES, remaining > dataSize? dataSize : remaining);
					// add the size...
					System.arraycopy(BytesUtil.longToBytes(read), 0, buffer, 0, Long.BYTES);
					// now send...
					os.write(buffer, 0, read);
					// done...
					bytesSent += read + Long.BYTES;
					remaining -= read;
				}
				stats.getStats().add(new TransferStatValue(
						TransferStatValue.Type.TransferBytes, bytesSent, TransferStatValue.Unit.Bytes));
				stats.getStats().add(new TransferStatValue(
						TransferStatValue.Type.TransferTime, System.currentTimeMillis() - startTime, TransferStatValue.Unit.Milliseconds));
				return stats;
			}
			
			finally {
				if(fis != null) {
					fis.close();
				}
			}
		}
		
	}
	
}
