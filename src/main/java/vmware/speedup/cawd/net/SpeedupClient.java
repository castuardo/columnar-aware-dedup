package vmware.speedup.cawd.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.net.SpeedupStreamer.TransferStats;


public class SpeedupClient extends Thread {

	private static final Logger logger = LogManager.getLogger(SpeedupClient.class);
	
	private String host = null;
	private int port = 0;
	private int connectionTimeout = 0;
	private int sendTimeout = 0;
	private String input = null;
	private SpeedupStreamer streamer = null;
	
	public SpeedupClient(int id, String host, int port, int connectionTimeout, int sendTimeout, String input, SpeedupStreamer streamer) {
		setName("SpeedupClient[" + id + "]");
		this.host = host;
		this.port = port;
		this.connectionTimeout = connectionTimeout;
		this.sendTimeout = sendTimeout;
		this.input = input;
		this.streamer = streamer;
	}
	
	private Socket connectUntilPossible() {
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(host, port), connectionTimeout);
			socket.setSoTimeout(sendTimeout);
			socket.setTcpNoDelay(true);
			return socket;
		}
		catch(Exception e) {
			logger.info("Connection failed, trying again in a while...");
			try {
				Thread.sleep(5000);
			}
			catch(Exception iDontCare) {}
		}
		return null;
	}
	
	@Override
	public void run() {
		// start logging something...
		logger.info("{} starting on host={}, port={}, connto={}, sendto={}, inputDir={}, streamerClass={}", 
				getName(), host, port, connectionTimeout, sendTimeout, input, streamer.getClass().getName());
		Socket socket = null;
		File inputFile = new File(input);
		
		// if no input directory, then get out
		if(!inputFile.exists()) {
			logger.error("{} is not a valid firle/directory, exiting...");
			return;
		}
		
		try {
			// try to open socket for sending first...
			socket = connectUntilPossible();
			
			// now get the input files
			List<File> files = null;
			
			if(inputFile.isDirectory()) {
				files = Arrays.asList(inputFile.listFiles());
			}
			else {
				files = Arrays.asList(inputFile);
			}
			
			logger.info("Going to process: {}", Arrays.toString(files.toArray()));
			
			// now do the hustle..
			if(files != null && files.size() > 0) {
				for(File file : files) {
					if(file.getName().endsWith(".parquet")) {
						// transfer this file...
						TransferStats stats = streamer.transferFile(file.getAbsolutePath(), 
								new BufferedInputStream(socket.getInputStream()),
								new BufferedOutputStream(socket.getOutputStream()));
						// log stats
						logger.info("{}", stats);
					}
					else {
						logger.warn("Ignoring {}, is not a parquet file...", file.getAbsolutePath());
					}
				}
			}
			else {
				// if there is nothing in the directory then just wait...
				logger.debug("No files to process...");
			}
			
		}
		
		catch(Exception e) {
			logger.error("Client encountered uncaught exception...", e);
		}
		
		finally {
			if(socket != null && !socket.isClosed()) {
				try {
					socket.close();
				}
				catch(Exception e) {
					logger.error("Could not close socket...", e);
				}
			}
			logger.info("{} exiting...", getName());
		}
	}
	
}
