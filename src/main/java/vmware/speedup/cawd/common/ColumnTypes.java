package vmware.speedup.cawd.common;

public class ColumnTypes {

	public static enum ORCColumnType {
		Double,
		Float,
		String,
		Other;
		
		public static ORCColumnType getTypeByName(String name) {
			switch(name) {
				case "double" : return Double;
				case "float"  : return Float;
				case "string"  : return String;
				default       : return Other;
			}
		}
	}
	
}
