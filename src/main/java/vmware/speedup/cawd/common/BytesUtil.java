package vmware.speedup.cawd.common;

public class BytesUtil {   
	 
	 public static byte[] longToBytes(long lng) {
		 byte[] b = new byte[] {
			       (byte) lng,
			       (byte) (lng >> 8),
			       (byte) (lng >> 16),
			       (byte) (lng >> 24),
			       (byte) (lng >> 32),
			       (byte) (lng >> 40),
			       (byte) (lng >> 48),
			       (byte) (lng >> 56)};
		 return b;
	 }

	 public static long bytesToLong(byte[] b) {
		 long ret = ((long) b[7] << 56)
	       | ((long) b[6] & 0xff) << 48
	       | ((long) b[5] & 0xff) << 40
	       | ((long) b[4] & 0xff) << 32
	       | ((long) b[3] & 0xff) << 24
	       | ((long) b[2] & 0xff) << 16
	       | ((long) b[1] & 0xff) << 8
	       | ((long) b[0] & 0xff);
		 return ret;
	 }
	
}

