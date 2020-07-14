package vmware.speedup.cawd.main;

import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vmware.speedup.cawd.net.SpeedupReceiver;
import vmware.speedup.cawd.net.SpeedupServer;
import vmware.speedup.cawd.net.SpeedupStreamer;


public class ParquetServerMain {

	private static final Logger logger = LogManager.getLogger(ParquetServerMain.class);
	private static final String R_TYPE = "cawd.receiver.type";
	
	public static void main(String[] args) {
		ParquetServerMain main = new ParquetServerMain();
		logger.info("Creating server...");
		try {
			SpeedupServer server = main.createServer(0, System.getProperties());
			// start it...
			server.start();
			// and wait...
			server.join();
		}
		catch(Exception e) {
			logger.error("Uncaught error when running server...", e);
		}
	}
	
	private SpeedupServer createServer(int id, Properties props) throws Exception {
		String host = props.getProperty(SpeedupServer.S_HOST, "127.0.0.1");
		int port = Integer.valueOf(props.getProperty(SpeedupServer.S_PORT, "2000"));
		String output = props.getProperty(SpeedupServer.S_OUT, "/tmp/server");
		SpeedupReceiver receiver = (SpeedupReceiver)(
				Class.forName(
						System.getProperty(R_TYPE, "vmware.speedup.cawd.net.SpeedupReceiver$PlainSpeedupReceiver")).newInstance());
		return new SpeedupServer(id, host, port, output, receiver);
	}
	
}
