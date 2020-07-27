package vmware.speedup.cawd.main;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.AbstractMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.FileUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import vmware.speedup.cawd.alluxio.Cache;
import vmware.speedup.cawd.alluxio.FileCache;
import vmware.speedup.cawd.alluxio.ChunkCache;

public class AlluxioSim {
	private static final Logger logger = LogManager.getLogger(AlluxioSim.class);

    private static String pathLocalize(String hdfsUrl){
        return hdfsUrl.replace("hdfs://node0:9000/user/hive/warehouse", "/home/administrator/spark-tpc-ds-performance-test/parquetdata");
    }

    public void run(String... args) throws IOException{
        byte[] rawdata = Files.readAllBytes(Paths.get("/home/administrator/spark-tpc-ds-performance-test/log/trimed.log"));
        Map<String, List<String>> logsMap = new ObjectMapper().readValue(rawdata, TreeMap.class);
        
        List<Cache> caches = new ArrayList<Cache>();
        caches.add(new FileCache());
        caches.add(new ChunkCache());
        
        int cnt = 0;
        List<String> lastList = null;
        for (Map.Entry<String, List<String>> entry : logsMap.entrySet()) {
            String time = entry.getKey();
            List<String> curList = entry.getValue();
            // logger.info(time + ": " + curList.toString());
            
            String op = curList.get(0);
            int nodeIdx = Integer.parseInt(curList.get(1));
            String path = pathLocalize(curList.get(2));

            if(!curList.equals(lastList)){
                logger.info(curList.toString());
                for(Cache c : caches){
                    if(op.equals("readfromUfs")){
                        c.readFile(path);
                    }
                    else if(op.equals("evictBlock")){
                        c.evictFile(path);
                    }
                    else if(op.equals("writetoUfs")){
                        c.writeFile(path);
                    }
                    else{
                        logger.error("unknown op = {}", op);
                    }
                }
            }
            if(cnt % 32 == 0){
                for(Cache c : caches){
                    logger.info(c.toString());
                }
            }
            // if(cnt >= 320){
            //     break;
            // }
            lastList = curList;
            cnt += 1;
        }

        for(Cache c : caches){
            logger.info(c.toString());
        }
    }

	public static void main(String... args) {
        try {
            AlluxioSim alluxiosim = new AlluxioSim();
            alluxiosim.run(args);
        } catch (IOException e) {
            logger.info(e.toString());
            logger.error("Alluxio Sim error");
        }
	}
}
