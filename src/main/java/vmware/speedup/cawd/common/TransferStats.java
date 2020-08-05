package vmware.speedup.cawd.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
	
	public static TransferStats globalStats(List<TransferStats> all) {
		TransferStats aggregated = new TransferStats("general");
		for(TransferStats stats : all) {
			TransferStats aggPerFile = aggregate(stats);
			aggregated.appendStats(aggPerFile);
		}
		return aggregate(aggregated);
		
		
	}
	
	public static TransferStats aggregate(TransferStats other) {
		TransferStats aggregated = new TransferStats(other.filePath);
		Map<TransferStatValue.Type, TransferStatValue> tmp = new HashMap<TransferStatValue.Type, TransferStatValue>();
		for(TransferStatValue value : other.getStats()) {
			if(tmp.containsKey(value.type)) {
				TransferStatValue current = tmp.get(value.type);
				current.value += value.value;
				current.ocurrences += 1;
				current.addValue(value.value);
			}
			else {
				tmp.put(value.type, value);
				value.addValue(value.value);
			}
		}
		// now add 
		for(TransferStatValue vv : tmp.values()) {
			if(vv.values.size() > 1) {
				Collections.sort(vv.values);
			}
			aggregated.getStats().add(vv);
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
			FileBytes,
			TransferBytes,
			TotalBytes,
			TransferTime,
			ExtraTransferBytes,
			DedupBytes,
			ParsingOverhead,
			StripeHit,
			StripeMiss,
			ColumnHit,
			DoubleColumnHit,
			DoubleColumnMiss,
			DoubleColumnMissBytes,
			StringColumnMiss,
			StringColumnMissBytes,
			ColumnMiss,
			FooterHit,
			FooterMiss,
			SmallColumn,
			StripeSize,
			FooterSize,
		}
		
		public enum Unit {
			Bytes,
			Milliseconds,
			Count
		};
		
		private int ocurrences = 0;
		private List<Double> values = new ArrayList<Double>();
		private Type type = null;
		private double value = 0.0;
		private Unit unit = null;
		
		public TransferStatValue(Type type, double value, Unit unit) {
			this.type = type;
			this.value = value;
			this.unit = unit;
			this.ocurrences = 1;
		}
		
		public double percentile(double percentile) {
		    int index = (int) Math.ceil(percentile / 100.0 * values.size());
		    return values.get(index-1);
		}
		
		public void addValue(double value) {
			this.values.add(value);
		}
		
		@Override
		public String toString() {
			StringBuilder bld =  new StringBuilder().append(type.name())
					.append("=")
					.append(value)
					.append(" ")
					.append(unit.name())
					.append(" (")
					.append(ocurrences)
					.append(")");
			if(ocurrences > 1 && values.size() > 1) {
				double min = values.get(0);
				double p25 = percentile(25);
				double p50 = percentile(25);
				double p75 = percentile(25);
				double max = values.get(values.size() - 1);
				bld.append(String.format(" (min, p25, p50, p75, max) = (%.3f,%.3f,%.3f,%.3f,%.3f)", min, p25, p50, p75, max));
			}
			return bld.toString();
		}
	}
	
}
