package vmware.speedup.cawd.parquet.dedup;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.eyalzo.common.chunks.PackChunking;

import vmware.speedup.cawd.dedup.ChunkingAlgorithm;

public class FileChunkingParquetChunkingAlgorithm extends ChunkingAlgorithm<FileChunkingParquetChunkingAlgorithm.ParquetFileChunk> {
    private static final Logger logger = LogManager.getLogger(FileChunkingParquetChunkingAlgorithm.class);
 
    @Override
	public List<ParquetFileChunk> eagerChunking(String fileName) throws IOException {
        List<ParquetFileChunk> identifiedChunks = new ArrayList<ParquetFileChunk>();
        logger.debug("Open parquet file: " + fileName);
        File tmp = new File(fileName);
        InputStream is = new FileInputStream(tmp);
        try {
            byte[] rawbytes = new byte[(int)tmp.length()];
            is.read(rawbytes, 0, rawbytes.length);

            LinkedList<Long> curChunkList = new LinkedList<Long>();
            PackChunking pack = new PackChunking(12);
            pack.getChunksAll(curChunkList, rawbytes, 0, rawbytes.length);

            long curPos = 0;
            int rawLenCumu = 0;
            for (Long curChunk : curChunkList) {
                int curChunkLen = PackChunking.chunkToLen(curChunk);
                identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.DataPageV1, curPos, curChunkLen));
                curPos += curChunkLen;
                rawLenCumu += curChunkLen;
            }
            if(rawLenCumu != rawbytes.length){
                logger.error("PackChunking chunks total size {}, should be {}", rawLenCumu, rawbytes.length);
            }
            
            return identifiedChunks;
        }
        finally {
            if(is != null) is.close();
        }
    }
	
    public static class ParquetFileChunk extends ChunkingAlgorithm.Chunk {
		
		public static enum ChunkType {
            ParquetHeader, // 4bytes, "PAR1"
            PageHeader, // per-page header
            DictPage, // per-column-chunk, for encoding use, optional
            DataPageV1, // per-column-chunk, including r-value, d-value, and actual data array -- all three mixed and compressed together
            DataPageV2, // per-column-chunk, including r-value, d-value, and actual data array -- only data array is compressed
            ParquetFooter, // per-file, Parquet file meta data
            AfterFooter, // per-file, 8bytes, including fileMetaData length and "PAR1"
            RepetitionValues, // per-page, r-value array
            DefinitionValues, // per-page, d-value array
            DataValues, // per-page, actual data array
            ERROR
		}
		
		public static ChunkType fromOrdinal(int ordinal) {
			switch(ordinal) {
				case 1: return  ChunkType.ParquetHeader;
				case 2: return  ChunkType.PageHeader;
				case 3: return  ChunkType.DictPage;
				case 4: return  ChunkType.DataPageV1;
				case 5: return  ChunkType.DataPageV2;
                case 6: return  ChunkType.ParquetFooter;
				case 7: return  ChunkType.AfterFooter;
				case 8: return  ChunkType.RepetitionValues;
				case 9: return  ChunkType.DefinitionValues;
                case 10: return  ChunkType.DataValues;
                default: return ChunkType.ERROR;
            }
        }
        
        public static int toOrdinal(ChunkType type) {
			switch(type) {
				case ParquetHeader: return 1;
				case PageHeader: return  2;
                case DictPage: return 3;
				case DataPageV1: return 4;
				case DataPageV2: return  5;
                case ParquetFooter: return 6;
				case AfterFooter: return 7;
				case RepetitionValues: return 8;
				case DefinitionValues: return 9;
				case DataValues: return 10;
				default: return -1000;
			}
		}
		
		private byte[] signature = null;
		private byte[] content = null;
		private ChunkType type = null;
		private long start = 0;
		private long size = 0;
		
        public ParquetFileChunk(ChunkType type, long start, long size) {
			this.signature = null;
			this.content = null;
			this.type = type;
			this.start = start;
			this.size = size;
        }
        
        public ParquetFileChunk(ChunkType type, long start, long size, byte[] content) {
			this.signature = null;
			this.content = content;
			this.type = type;
			this.start = start;
			this.size = size;
		}
        
		public ParquetFileChunk(byte[] signature) {
			this.signature = signature;
		}
		
		public ParquetFileChunk(byte[] signature, byte[] content) {
			this.signature = signature;
			this.content = content;
		}

		public ChunkType getType() {
			return type;
		}

		public long getStart() {
			return start;
		}

		public long getSize() {
			return size;
		}

		public byte[] getSignature() {
			return signature;
		}

		public byte[] getContent() {
			return content;
		}

		public void setSignature(byte[] signature) {
			this.signature = signature;
		}

		public void setContent(byte[] content) {
			this.content = content;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ParquetFileChunk [type=").append(type.name()).append(", start=").append(start).append(", size=")
					.append(size).append(", signature=").append(signature != null? Arrays.toString(signature) : "none").append("]");
			return builder.toString();
		}

		@Override
		public int doHashCode() {
			return Arrays.hashCode(signature);
		}

		@Override
		public boolean doEquals(Object other) {
			if(other instanceof ParquetFileChunk) {
				return Arrays.equals(signature, ((ParquetFileChunk)other).signature); 
			}
			return false;
		}
		
	}

}