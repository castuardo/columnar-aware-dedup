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
	public static final String C_HOST = "cawd.client.host";
	public static final String C_PORT = "cawd.client.port";
	public static final String C_IN = "cawd.client.input";
	public static final String C_FILTERS = "cawd.client.filters";
	
	private String host = null;
	private int port = 0;
	private String input = null;
	private List<String> fileFilters = null;
	private SpeedupStreamer streamer = null;
	
	public SpeedupClient(
			int id, String host, int port,String input, SpeedupStreamer streamer, List<String> fileFilters) {
		setName("SpeedupClient[" + id + "]");
		this.host = host;
		this.port = port;
		this.input = input;
		this.streamer = streamer;
		this.fileFilters.addAll(fileFilters);
	}
	
	private Socket connectUntilPossible() {
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(host, port), 0);
			socket.setSoTimeout(0);
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
	
	private boolean isInFilter(String file) {
		for(String filter : fileFilters) {
			if(file.endsWith(filter)) return true;
		}
		return false;
	}
	
	@Override
	public void run() {
		// start logging something...
		logger.info(
				"{} starting on host={}, port={}, inputDir={}, streamerClass={}, filters={}", 
				getName(), host, port, input, streamer.getClass().getName(), Arrays.toString(fileFilters.toArray()));
		
		Socket socket = null;
		File inputFile = new File(input);
		// if no input file/directory, then get out
		if(!inputFile.exists()) {
			logger.error("{} is not a valid file/directory, exiting...");
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
					if(isInFilter(file.getName())) {
						// transfer this file...
						TransferStats stats = streamer.transferFile(
								file.getAbsolutePath(), 
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
