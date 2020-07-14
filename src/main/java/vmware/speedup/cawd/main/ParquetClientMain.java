package vmware.speedup.cawd.main;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vmware.speedup.cawd.net.SpeedupClient;
import vmware.speedup.cawd.net.SpeedupStreamer;

public class ParquetClientMain {

	private static final Logger logger = LogManager.getLogger(ParquetClientMain.class);
	private static final String S_TYPE = "cawd.streamer.type";

	public static void main(String[] args) {
		ParquetClientMain main = new ParquetClientMain();
		logger.info("Creating client...");
		try {
			// create it...
			SpeedupClient client = main.createClient(0, System.getProperties());
			// start it...
			client.start();
			// and wait...
			client.join();
		}
		catch(Exception e) {
			logger.error("Uncaught error when running client...", e);
		}
	}
	
	private SpeedupClient createClient(int id, Properties props) throws Exception {
		String host = props.getProperty(SpeedupClient.C_HOST, "127.0.0.1");
		int port = Integer.valueOf(props.getProperty(SpeedupClient.C_PORT, "2000"));
		String input = props.getProperty(SpeedupClient.C_IN, "/tmp/client");
		List<String> filters = Arrays.asList(props.getProperty(SpeedupClient.C_FILTERS, ".parquet").split(","));
		SpeedupStreamer streamer = (SpeedupStreamer)(
				Class.forName(
						System.getProperty(S_TYPE, "vmware.speedup.cawd.net.SpeedupStreamer$PlainSpeedupStreamer")).newInstance());
		return new SpeedupClient(id, host, port, input, streamer, filters);
	}
}
