package vmware.speedup.cawd.parquet.parser;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.parquet.cli.BaseCommand;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.InputFile;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.charset.Charset;

import static org.apache.parquet.cli.Util.columnName;
import static org.apache.parquet.cli.Util.descriptor;
import static org.apache.parquet.cli.Util.encodingAsString;
import static org.apache.parquet.cli.Util.humanReadable;
import static org.apache.parquet.cli.Util.minMaxAsString;
import static org.apache.parquet.cli.Util.primitive;
import static org.apache.parquet.cli.Util.shortCodec;
import static org.apache.parquet.bytes.BytesUtils.writeIntLittleEndian;

 
public class ParquetParser {
    
    private static final Logger logger = LogManager.getLogger(ParquetParser.class);
    private String parquetfilepath;
    private ParquetFileReader reader;
    private ParquetMetadata footer;
    private int curRowGroupNum;
    private Map<ColumnDescriptor, PrimitiveType> columns;
    private static ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    
    public static final String MAGIC_STR = "PAR1";
    public static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));
    public static final int CURRENT_VERSION = 1;

	public ParquetParser(String parquetfilepath) {
        this.parquetfilepath = parquetfilepath;
        this.curRowGroupNum = 0;
        try {
            Configuration conf = new Configuration();
            InputFile inputfile = HadoopInputFile.fromPath(new Path(parquetfilepath), conf);            
            this.reader = ParquetFileReader.open(inputfile);
        } catch (IOException e) {
            logger.error("File open error: " + parquetfilepath);
        }
        this.footer = this.reader.getFooter();
        MessageType schema = this.footer.getFileMetaData().getSchema();
        this.columns = Maps.newLinkedHashMap();
        for (ColumnDescriptor descriptor : schema.getColumns()) {
            this.columns.put(descriptor, primitive(schema, descriptor.getPath()));
        }

        logger.info("Open parquet file: " + parquetfilepath);
    }
    
    public enum ChunkType {
        PAR_HEADER,
        PAGE_HEADER,
        DICTPAGE,
        DATAPAGE,
        REPETITION_VALUES,
        DEFINITION_VALUES,
        DATA_VALUES,
        PAR_FOOTER,
        AFTER_FOOTER
    }
    public class Chunk {
        public ChunkType ct;
        public InputStream in;
        public Chunk(ChunkType _ct, InputStream _in) {
            this.ct = _ct;
            this.in = _in;
        }
    }
    public Chunk GetParquetHeader() {
        Chunk header_chunk = new Chunk(ChunkType.PAR_HEADER, new ByteArrayInputStream(MAGIC));
        return header_chunk;
    }

    public Chunk GetParquetFooter() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        org.apache.parquet.format.FileMetaData parquetMetadata = this.reader.getParquetFileMetaData();
        Util.writeFileMetaData(parquetMetadata, out);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Chunk chunk = new Chunk(ChunkType.PAR_FOOTER, in);
        return chunk;
    }

    public Chunk GetAfterFooter() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeIntLittleEndian(out, ParquetFileReader.MyFooterLength);
        out.write(MAGIC);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Chunk chunk = new Chunk(ChunkType.AFTER_FOOTER, in);
        return chunk;
    }

    public List<Chunk> GetNextRowGroupChunks() throws IOException {
        List<Chunk> chunks = new ArrayList<Chunk>();

        // reading the whole next row group including multiple chunks and each chunk includes multiple pages (dict page or data page)
        PageReadStore pageStore = this.reader.readNextRowGroup();
        if(pageStore == null){
            return null;
        }

        for (ColumnDescriptor descriptor : this.columns.keySet()) {
            PageReader pages = pageStore.getPageReader(descriptor);
            PageHeader pageHeader;
            while((pageHeader = pages.readPageHeader()) != null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Util.writePageHeader(pageHeader, out);
                ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
                chunks.add(new Chunk(ChunkType.PAGE_HEADER, in));

                DataPage datapage;
                DictionaryPage dictpage;
                BytesInput data;
                switch (pageHeader.type) {
                    case DICTIONARY_PAGE:
                        dictpage = pages.readRawDictionaryPage();
                        data = dictpage.getBytes();
                        chunks.add(new Chunk(ChunkType.DICTPAGE, data.toInputStream()));
                        break;
                    case DATA_PAGE:
                        datapage = pages.readRawPage();
                        data = ((DataPageV1) datapage).getBytes();
                        chunks.add(new Chunk(ChunkType.DATAPAGE, data.toInputStream()));
                        break;
                    case DATA_PAGE_V2:
                        datapage = pages.readRawPage();
                        data = ((DataPageV2) datapage).getData();
                        chunks.add(new Chunk(ChunkType.DATAPAGE, data.toInputStream()));
                        break;
                    default:
                        logger.error("skipping page of type {} of size {}", pageHeader.getType(), pageHeader.compressed_page_size);
                        break;
                }   
            }
        }
        this.curRowGroupNum+= 1;
        return chunks;
    }

    public void ShowParquetColumn() throws IOException {
        MessageType schema = this.reader.getFileMetaData().getSchema();
        Map<ColumnDescriptor, PrimitiveType> columns = Maps.newLinkedHashMap();
        for (ColumnDescriptor descriptor : schema.getColumns()) {
            columns.put(descriptor, primitive(schema, descriptor.getPath()));
        }

        CompressionCodecName codec = this.reader.getRowGroups().get(0).getColumns().get(0).getCodec();
        // accumulate formatted lines to print by column
        Map<String, List<String>> formatted = Maps.newLinkedHashMap();
        PageFormatter formatter = new PageFormatter();
        PageReadStore pageStore;
        int rowGroupNum = 0;

        File f = new File(parquetfilepath + ".datapages");
        OutputStream fos = new FileOutputStream(f, false); // not appending
        
        long expected_filesize = 0;

        while ((pageStore = this.reader.readNextRowGroup()) != null) {
          for (ColumnDescriptor descriptor : columns.keySet()) {
            List<String> lines = formatted.get(columnName(descriptor));
            if (lines == null) {
              lines = Lists.newArrayList();
              formatted.put(columnName(descriptor), lines);
            }

            formatter.setContext(rowGroupNum, columns.get(descriptor), codec);
            PageReader pages = pageStore.getPageReader(descriptor);

            DictionaryPage dict = pages.readDictionaryPage();
            if (dict != null) {
              lines.add(formatter.format(dict));
            }
            DataPage page;
            while ((page = pages.readPage()) != null) {
                lines.add(formatter.format(page));
                BytesInput data = null;
                if (page instanceof DataPageV1) {
                    data = ((DataPageV1)page).getBytes();
          
                }
                else if(page instanceof DataPageV2){
                    data = ((DataPageV2)page).getData();
                }
                else{
                    logger.info("Not v1 or V2");
                }
                if(page != null){
                    long totalSize = page.getCompressedSize();
                    expected_filesize += totalSize;
                    data.writeAllTo(fos);
                    // logger.info(page.toString());
                }
            }
          }
          rowGroupNum += 1;
        }
        fos.close();
        logger.info("expected file size " + humanReadable(expected_filesize));

        // TODO: Show total column size and overall size per value in the column summary line
        for (String columnName : formatted.keySet()) {
          logger.info(String.format("\nColumn: %s\n%s", columnName, StringUtils.leftPad("", 80, '-')));
          logger.info(formatter.getHeader());
          for (String line : formatted.get(columnName)) {
            logger.info(line);
          }
          logger.info("");
        }
    }

    private class PageFormatter implements DataPage.Visitor<String> {
        private int rowGroupNum;
        private int pageNum;
        private PrimitiveType type;
        private String shortCodec;
    
        String getHeader() {
          return String.format("  %-6s %-5s %-4s %-7s %-10s %-10s %-8s %-7s %s",
              "page", "type", "enc", "count", "avg size", "size", "rows", "nulls", "min / max");
        }
    
        void setContext(int rowGroupNum, PrimitiveType type, CompressionCodecName codec) {
          this.rowGroupNum = rowGroupNum;
          this.pageNum = 0;
          this.type = type;
          this.shortCodec = shortCodec(codec);
        }
    
        String format(Page page) {
          String formatted = "";
          if (page instanceof DictionaryPage) {
            formatted = printDictionaryPage((DictionaryPage) page);
          } else if (page instanceof DataPage) {
            formatted = ((DataPage) page).accept(this);
          }
          pageNum += 1;
          return formatted;
        }
    
        private String printDictionaryPage(DictionaryPage dict) {
          // TODO: the compressed size of a dictionary page is lost in Parquet
          dict.getUncompressedSize();
          long totalSize = dict.getCompressedSize();
          int count = dict.getDictionarySize();
          float perValue = ((float) totalSize) / count;
          String enc = encodingAsString(dict.getEncoding(), true);
          if (pageNum == 0) {
            return String.format("%3d-D    %-5s %s %-2s %-7d %-10s %-10s",
                rowGroupNum, "dict", shortCodec, enc, count, humanReadable(perValue),
                humanReadable(totalSize));
          } else {
            return String.format("%3d-%-3d  %-5s %s %-2s %-7d %-10s %-10s",
                rowGroupNum, pageNum, "dict", shortCodec, enc, count, humanReadable(perValue),
                humanReadable(totalSize));
          }
        }
    
        @Override
        public String visit(DataPageV1 page) {
          String enc = encodingAsString(page.getValueEncoding(), false);
          long totalSize = page.getCompressedSize();
          int count = page.getValueCount();
          String numNulls = page.getStatistics().isNumNullsSet() ? Long.toString(page.getStatistics().getNumNulls()) : "";
          float perValue = ((float) totalSize) / count;
          String minMax = minMaxAsString(page.getStatistics(), type.getOriginalType());
          return String.format("%3d-%-3d  %-5s %s %-2s %-7d %-10s %-10s %-8s %-7s %s",
              rowGroupNum, pageNum, "data", shortCodec, enc, count, humanReadable(perValue),
              humanReadable(totalSize), "", numNulls, minMax);
        }
    
        @Override
        public String visit(DataPageV2 page) {
          String enc = encodingAsString(page.getDataEncoding(), false);
          long totalSize = page.getCompressedSize();
          int count = page.getValueCount();
          int numRows = page.getRowCount();
          int numNulls = page.getNullCount();
          float perValue = ((float) totalSize) / count;
          String minMax = minMaxAsString(page.getStatistics(), type.getOriginalType());
          String compression = (page.isCompressed() ? shortCodec : "_");
          return String.format("%3d-%-3d  %-5s %s %-2s %-7d %-10s %-10s %-8d %-7s %s",
              rowGroupNum, pageNum, "data", compression, enc, count, humanReadable(perValue),
              humanReadable(totalSize), numRows, numNulls, minMax);
        }
    }
}