package vmware.speedup.cawd.main;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestMain {

	private static final Logger logger = LogManager.getLogger(TestMain.class);
	
	public static void main(String... args) {
		logger.info("Works fine ({})!", new Date().toString());
		logger.info("Works fine ({})!", new Date().toString());
	}
	
}
