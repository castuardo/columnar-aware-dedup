package vmware.speedup.cawd.orc.dedup;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

public class AlluxioClientTest {

private static final Logger logger = LogManager.getLogger(AlluxioClientTest.class);
	

    @Test
    public void doTest() {
	String file = "alluxio://ip-172-31-26-162.us-east-2.compute.internal:19998/other/test-1717-7.orc";
	logger.info("Trying out {}", file);
    	AlluxioURI uri = new AlluxioURI(file);
        InstancedConfiguration conf = InstancedConfiguration.defaults();
	conf.set(PropertyKey.MASTER_HOSTNAME, "ip-172-31-26-162.us-east-2.compute.internal");
    	conf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "ASYNC_THROUGH");
        FileOutStream writer = getWriter(uri, conf);
    	if(writer != null) {
    		try {
			logger.info("Got connected, writing..");
    			// do something here
    			writer.write("Hello, this is working!".getBytes());
    		}
    		catch(IOException e) {
    			logger.error("Error while writing...", e);
    		}
    		finally {
			logger.info("done!");
    			closeWriter(writer);
    		}
    	}
	else {
		logger.info("Error, never connected...");
	}
    	
    }
   

	public FileOutStream getWriter(AlluxioURI uri, AlluxioConfiguration configuration){
		try {
			FileSystem fs = FileSystem.Factory.create(configuration);
			return fs.createFile(uri);
		}
		catch(Exception e) {
			logger.error("Error while creating file", e);
			return null;
		}
	}

	public void closeWriter(FileOutStream out) {
		try {
			if(out != null) {
				out.flush();
				out.close();
			}
		}
		catch(IOException e) {
			logger.error("Error while closing client", e);
		}
	}
	
}

