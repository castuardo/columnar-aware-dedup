package vmware.speedup.cawd.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.TransferStats;

public class SpeedupServer extends Thread {

	private static final Logger logger = LogManager.getLogger(SpeedupServer.class);
	public static final String S_HOST = "cawd.server.host";
	public static final String S_PORT = "cawd.server.port";
	public static final String S_OUT = "cawd.server.outputFolder";
	
	private String host = null;
	private int port = 0;
	private File destinationFolder = null;
	private SpeedupReceiver receiver = null;
	
	public SpeedupServer(int id, String host, int port, String destinationFolder, SpeedupReceiver receiver) {
		setName("SpeedupServer[" + id + "]");
		this.host = host;
		this.port = port;
		this.destinationFolder = new File(destinationFolder);
		this.receiver = receiver;
	}
	
	
	@Override
	public void run() {
		// start logging something...
		logger.info(
				"{} starting on host={}, port={}, oDir={}, receiverClass={}", 
				getName(), host, port, destinationFolder, receiver.getClass().getName());
		// create the output directory...
		if(!destinationFolder.exists()) {
			destinationFolder.mkdirs();
		}
		// so here we create a server socket and we just bind it
		ServerSocket serverSocket = null;
		Socket connection = null;
		try {
			serverSocket = new ServerSocket(port, 0, InetAddress.getByName(host));
			connection = serverSocket.accept();
			connection.setKeepAlive(true);
			connection.setSoTimeout(0);
			connection.setTcpNoDelay(true);
			logger.debug("Accepted connection...");
			InputStream is = new BufferedInputStream(connection.getInputStream());
			OutputStream os = new BufferedOutputStream(connection.getOutputStream());
			while(true) {
				try {
					TransferStats stats = receiver.receiveFile(destinationFolder.getAbsolutePath(), is, os);
					if(stats != null) {
						logger.info("{}", stats);
					}
					else {
						break;
					}
				}
				catch(Exception e) {
					logger.error("Server found an uncaught error and will exit...", e);
					break;
				}
			}
		}
		catch(Exception e) {
			logger.error("Exception thrown while accepting connection...", e);
		}
		finally {
			try{
				if(connection != null) connection.close();
			}
			catch(Exception e) {}
			try {
				if(serverSocket != null) serverSocket.close();
			}
			catch(Exception e) {}
			logger.info("{} exiting...", getName());
		}
		
	}
	
}
