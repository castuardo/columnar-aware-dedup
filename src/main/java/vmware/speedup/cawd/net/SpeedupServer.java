package vmware.speedup.cawd.net;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vmware.speedup.cawd.common.BytesUtil;

public class SpeedupServer extends Thread {

	private static final Logger logger = LogManager.getLogger(SpeedupServer.class);
	public static final String S_HOST = "cawd.server.host";
	public static final String S_PORT = "cawd.server.port";
	public static final String S_OUT = "cawd.server.outputFolder";
	
	private String host = null;
	private int port = 0;
	private File destinationFolder = null;
	
	public SpeedupServer(int id, String host, int port, String destinationFolder) {
		setName("SpeedupClient[" + id + "]");
		this.host = host;
		this.port = port;
		this.destinationFolder = new File(destinationFolder);
	}
	
	private boolean handleIncommingFile(InputStream stream) throws IOException {
		// first, size and name
		byte[] sizeBuffer = new byte[Long.BYTES];
		String fileName = null;
		stream.read(sizeBuffer, 0, Long.BYTES);
		long size = BytesUtil.bytesToLong(sizeBuffer);
		FileOutputStream fos = null;
		// now the name...
		if(size > 0) {
			try {
				byte [] name = new byte[(int)size];
				stream.read(name, 0, (int)size);
				fileName = new String(name);
				stream.read(sizeBuffer, 0, Long.BYTES);
				long fileSize = BytesUtil.bytesToLong(sizeBuffer);
				logger.info("Incomming file: {} of size {}", name, fileSize);
				// now, read the whole file
				long received = 0;
				// now, lets write
				fos = new FileOutputStream(new File(destinationFolder.getAbsolutePath() + File.separator + fileName));
				while(received < fileSize) {
					// read the chunk size
					stream.read(sizeBuffer, 0, Long.BYTES);
					long chunkSize = BytesUtil.bytesToLong(sizeBuffer);
					byte [] chunk = new byte[(int)chunkSize];
					stream.read(chunk, 0, chunk.length);
					// write it back
					fos.write(chunk, 0, chunk.length);
					received += chunkSize;
				}
				// done
				logger.info("Done with file={}, received={} bytes...", fileName, received);
				return true;
			}
			finally {
				if(fos != null) fos.close();
			}
		}
		else {
			logger.info("Terminating server...");
			// get out
			return false;
		}
		
	}
	
	@Override
	public void run() {
		// create the output directory...
		if(!destinationFolder.exists()) {
			destinationFolder.mkdirs();
		}
		// so here we create a server socket and we just bind it
		try {
			ServerSocket serverSocket = new ServerSocket(port, 0, InetAddress.getByName(host));
			Socket connection = serverSocket.accept();
			connection.setSoTimeout(0);
			while(true) {
				try {
					// and now handle messages
					if(!handleIncommingFile(new BufferedInputStream(connection.getInputStream()))) {
						break;
					}
				}
				catch(Exception e) {
					logger.error("Server found an uncaugh error and will exit...", e);
					break;
				}
				finally {
					if(serverSocket != null) serverSocket.close();
					if(connection != null) connection.close();	
				}
			}
		}
		catch(Exception e) {
			logger.error("Exception thrown while accepting connection...", e);
		}
		finally {
			logger.info("{} exiting...", getName());
		}
		
	}
	
}
