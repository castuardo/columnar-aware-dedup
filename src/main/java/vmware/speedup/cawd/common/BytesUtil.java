package vmware.speedup.cawd.common;

import java.nio.ByteBuffer;

public class BytesUtil {

	 private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);    
	 
	 public static byte[] longToBytes(long x) {
	        buffer.putLong(0, x);
	        return buffer.array();
	 }

	 public static long bytesToLong(byte[] bytes) {
		 buffer.put(bytes, 0, Long.BYTES);
		 buffer.flip();
		 return buffer.getLong();
	 }
	
}

