package vmware.speedup.cawd.net;

import java.io.DataInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.common.BytesUtil;
import vmware.speedup.cawd.common.TransferStats;


public class SpeedupClient extends Thread {

	private static final Logger logger = LogManager.getLogger(SpeedupClient.class);
	public static final String C_HOST = "cawd.client.host";
	public static final String C_PORT = "cawd.client.port";
	public static final String C_IN = "cawd.client.input";
	public static final String C_FILTERS = "cawd.client.filters";
	
	private String host = null;
	private int port = 0;
	private String input = null;
	private List<String> fileFilters = new ArrayList<String>();
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
	
	private Socket connect() {
		while(true){
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress(host, port), 0);
				socket.setSoTimeout(0);
				socket.setTcpNoDelay(true);
				socket.setKeepAlive(true);
				return socket;
			}
			catch(Exception e) {
				logger.info("Connection failed, trying again in a while...");
				try {
					Thread.sleep(5000);
				}
				catch(Exception iDontCare) {
					
				}
			}
		}
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
		
		List<TransferStats> allStats = new ArrayList<TransferStats>();

		try {
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
				// try to open socket for sending first...
				socket = connect();
				InputStream is = new DataInputStream(socket.getInputStream());
				OutputStream os = new BufferedOutputStream(socket.getOutputStream());
				for(File file : files) {
					if(isInFilter(file.getName())) {
						// transfer this file...
						TransferStats stats = streamer.transferFile(file.getAbsolutePath(), is, os);
						// log stats
						logger.info("{}", stats);
						allStats.add(stats);
					}
					else {
						logger.warn("Ignoring {}, is not a parquet file...", file.getAbsolutePath());
					}
				}
				logger.info("Done, sending termination signal...");
				// done with files, send finish signal
				os.write(BytesUtil.longToBytes(-1L));
				os.flush();
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
			logger.info("{}", TransferStats.globalStats(allStats));
			if(socket != null && !socket.isClosed()) {
				try {
					logger.debug("Closing socket...");
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
