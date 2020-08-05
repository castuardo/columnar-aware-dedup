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

public class AlluxioClientTest {

private static final Logger logger = LogManager.getLogger(AlluxioClientTest.class);
	

    @Test
    public void doTest() {
    	AlluxioURI uri = new AlluxioURI("");
    	FileOutStream writer = getWriter(uri, InstancedConfiguration.defaults());
    	if(writer != null) {
    		try {
    			// do something here
    			writer.write("Hello, this is working!".getBytes());
    		}
    		catch(IOException e) {
    			logger.error("Error while writing...", e);
    		}
    		finally {
    			closeWriter(writer);
    		}
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

