package vmware.speedup.cawd.common;

public class Comparators {

	
	public static class SignatureComparator implements java.util.Comparator<byte[]>{

		@Override
		public int compare(byte[] o1, byte[] o2) {
			int current = 0;
			for(byte bb : o1) {
				if(bb < o2[current]) return -1;
				if(bb > o2[current]) return 1;
				++current;
			}
			return 0;
		}
	}
	
}
