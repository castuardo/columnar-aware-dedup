package vmware.speedup.cawd.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JacksonInject.Value;


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
	
	public void appendStats(TransferStats other) {
		stats.addAll(other.getStats());
	}
	
	public static TransferStats aggregate(TransferStats other) {
		TransferStats aggregated = new TransferStats(other.filePath);
		Map<TransferStatValue.Type, TransferStatValue> tmp = new HashMap<TransferStatValue.Type, TransferStatValue>();
		for(TransferStatValue value : other.getStats()) {
			if(tmp.containsKey(value.type)) {
				TransferStatValue current = tmp.get(value.type);
				current.value += value.value;
				current.ocurrences += 1;
			}
			else {
				tmp.put(value.type, value);
			}
		}
		return aggregated;
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
			ExtraTransferBytes,
			DedupBytes
		}
		
		public enum Unit {
			Bytes,
			Milliseconds
		};
		
		private int ocurrences = 0;
		private Type type = null;
		private double value = 0.0;
		private Unit unit = null;
		
		public TransferStatValue(Type type, double value, Unit unit) {
			this.type = type;
			this.value = value;
			this.unit = unit;
			this.ocurrences = 1;
		}
		
		@Override
		public String toString() {
			return new StringBuilder().append(type.name())
					.append("=")
					.append(value)
					.append(" ")
					.append(unit.name())
					.append(" (")
					.append(ocurrences)
					.append(")")
					.toString();
		}
	}
	
}
