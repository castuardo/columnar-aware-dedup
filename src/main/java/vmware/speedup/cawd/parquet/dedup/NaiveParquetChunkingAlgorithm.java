package vmware.speedup.cawd.parquet.dedup;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import static org.apache.parquet.cli.Util.primitive;
import static org.apache.parquet.bytes.BytesUtils.writeIntLittleEndian;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import vmware.speedup.cawd.dedup.ChunkingAlgorithm;
 
public class NaiveParquetChunkingAlgorithm extends ChunkingAlgorithm<NaiveParquetChunkingAlgorithm.ParquetFileChunk> {
    private static final Logger logger = LogManager.getLogger(NaiveParquetChunkingAlgorithm.class);
 
    public static final String MAGIC_STR = "PAR1";
    public static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));
    
    @Override
	public List<ParquetFileChunk> eagerChunking(String fileName) throws IOException {
        List<ParquetFileChunk> identifiedChunks = new ArrayList<ParquetFileChunk>();

        Configuration conf = new Configuration();
        ParquetFileReader parquetReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fileName), conf));
        logger.info("Open parquet file: " + fileName);

        try {
            // read footer, aka, parquet file meta data
            ParquetMetadata footer = parquetReader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            
            // LinkedHashMap retains the insertion order. 
            Map<ColumnDescriptor, PrimitiveType> columns = new LinkedHashMap<ColumnDescriptor, PrimitiveType>();

            // read all column descriptors. 
            for (ColumnDescriptor descriptor : schema.getColumns()) {
                columns.put(descriptor, primitive(schema, descriptor.getPath()));
            }
            
            long curPos = 0;
            byte[] rawbytes;

            // the first chunk is the Magic chars
            identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.ParquetHeader, curPos, MAGIC.length));
            curPos += MAGIC.length;

            // reading the whole next row group including multiple chunks and each chunk includes multiple pages (dict page or data page)
            PageReadStore pageStore = null;
            while((pageStore = parquetReader.readNextRowGroup()) != null){ // for each row group
                for (ColumnDescriptor descriptor : columns.keySet()) { // for each column
                    PageReader pages = pageStore.getPageReader(descriptor);
                    PageHeader pageHeader;
                    while((pageHeader = pages.readPageHeader()) != null) { // for each page
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        Util.writePageHeader(pageHeader, out);
                        rawbytes = out.toByteArray();
                        // page meta data chunk
                        identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.PageHeader, curPos, rawbytes.length));
                        curPos += rawbytes.length;

                        switch (pageHeader.type) {
                            case DICTIONARY_PAGE:
                                rawbytes = pages.readRawDictionaryPage().getBytes().toByteArray();
                                // dict page
                                identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.DictPage, curPos, rawbytes.length));
                                curPos += rawbytes.length;
                                break;
                            case DATA_PAGE:
                                rawbytes = ((DataPageV1) pages.readRawPage()).getBytes().toByteArray();
                                // data page v1
                                identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.DataPageV1, curPos, rawbytes.length));
                                curPos += rawbytes.length;
                                break;
                            case DATA_PAGE_V2:
                                rawbytes = ((DataPageV2) pages.readRawPage()).getData().toByteArray();
                                // data page v2
                                identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.DataPageV2, curPos, rawbytes.length));
                                curPos += rawbytes.length;
                                break;
                            default:
                                logger.error("skipping page of type {} of size {}", pageHeader.getType(), pageHeader.compressed_page_size);
                                break;
                        }   
                    }
                }
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            org.apache.parquet.format.FileMetaData parquetMetadata = parquetReader.getParquetFileMetaData();
            Util.writeFileMetaData(parquetMetadata, out);
            rawbytes = out.toByteArray();
            // after all row group, we have parquet footer (parquet file meta data); 
            identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.ParquetFooter, curPos, rawbytes.length));
            curPos += rawbytes.length;

            out = new ByteArrayOutputStream();
            writeIntLittleEndian(out, ParquetFileReader.MyFooterLength);
            out.write(MAGIC);
            rawbytes = out.toByteArray();
            // after footer, there are eight bytes. 
            identifiedChunks.add(new ParquetFileChunk(ParquetFileChunk.ChunkType.AfterFooter, curPos, rawbytes.length));
            curPos += rawbytes.length;
            
            return identifiedChunks;
        }
        finally {
            if(parquetReader != null) parquetReader.close();
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