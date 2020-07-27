package vmware.speedup.cawd.alluxio;

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
import static org.apache.parquet.cli.Util.humanReadable;

public abstract class Cache {
    public long ufsReadBytes;
    public long ufsWriteBytes;
    public long cacheSize;
    public Cache() {
        ufsReadBytes = 0;
        ufsWriteBytes = 0;
        cacheSize = 0;
    }
    public void readFile(String path) throws IOException {
        
    }
    public void writeFile(String path) throws IOException {

    }
    public void evictFile(String path) throws IOException {
        
    }
    public String toString() {
        return new StringBuilder()
        .append("ufsReadBytes="+ humanReadable(ufsReadBytes) +" ") 
        .append("ufsWriteBytes="+ humanReadable(ufsWriteBytes) +" ") 
        .append("cacheSize="+ humanReadable(cacheSize)+" ") 
        // .append(String.format("; dedup ratio: %.3f%%, extra/totalTransfer = %.3f%%", (1 - totalTransSize / allFileSize) * 100, extraTransSize / totalTransSize * 100))
        .toString();
    }
}
