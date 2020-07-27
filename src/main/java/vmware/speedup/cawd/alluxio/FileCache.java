package vmware.speedup.cawd.alluxio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
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
import vmware.speedup.cawd.alluxio.Cache;

public class FileCache extends Cache {
	private static final Logger logger = LogManager.getLogger(FileCache.class);
    private Set<String> fileStore;
    public FileCache() {
        super();
        fileStore = new HashSet<String>();
    }
    @Override
    public void readFile(String path) throws IOException {
        if(!fileStore.contains(path)){
            fileStore.add(path);
            long filesize = new FileInputStream(path).available();
            ufsReadBytes += filesize;
            cacheSize += filesize;
        }
        fileStore.add(path);
    }
    @Override
    public void writeFile(String path) throws IOException {
        if(!fileStore.contains(path)){
            fileStore.add(path);
            long filesize = new FileInputStream(path).available();
            ufsWriteBytes += filesize;
            cacheSize += filesize;
        }
        fileStore.add(path);
    }
    @Override
    public void evictFile(String path) throws IOException {
        if(!fileStore.contains(path)){
            logger.error("evicted file not in filestore");
            return;
        }
        fileStore.remove(path);
        long filesize = new FileInputStream(path).available();
        cacheSize -= filesize;
    }
}
