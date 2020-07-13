package vmware.speedup.cawd.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TransferStats {

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
	
	public static class TransferStatValue {
		
		public enum Type {
			TransferBytes,
			TransferTime,
			ExtraTransferBytes
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
	
}
