package kairosdb.export.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import kairosdb.export.csv.conf.CommandCli;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import kairosdb.export.csv.utils.TimeUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;

public class ExportToCsv {

  private static Config config;
  //private static final Logger LOGGER = LoggerFactory.getLogger(ExportToCsv.class);
  private static HttpClient client;
  private static final String STORAGE_GROUP_PREFIX = "group_";
  private static final String DIR_NAME = "%s_%s_%s_";
  private static final String CSV_FILE_SUFFIX = ".csv";
  private static final String CSV_FILE_NAME = "%s_%s_%s_%d.csv";
  private static final String TSFILE_FILE_NAME = "%s_%s_%s_%d.tsfile";
  private static int storageGroupSize;
  private static final String PATH_TEMPLATE = "root.%s%s.%s";
  private static Map<Long, Map<String, Object>> dataTable = new LinkedHashMap<>();
  private static String[] metrics;
  private static String trainNumber;
  private static long startTime;
  private static long endTime;
  private static String tsFilePath;
  private static int column;
  private static String[] header;
  private static String dirAbsolutePath;


  public static void main(String[] args) {

    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    storageGroupSize = config.STORAGE_GROUP_SIZE;
    trainNumber = config.MACHINE_ID;
    column = config.COLUMN;
    header = config.METRIC_LIST.split(",");
    int column_loop = (int) Math.ceil((float) config.METRIC_LIST.split(",").length / column);
    String dirPath = String
        .format(DIR_NAME, config.MACHINE_ID, config.START_TIME, config.ENDED_TIME);
    dirAbsolutePath = config.EXPORT_FILE_DIR + File.separator + dirPath;
    if (!new File(dirAbsolutePath).exists()) {
      new File(dirAbsolutePath).mkdir();
    }

    boolean writeTest = false;
    boolean show = false;
    if (writeTest) {
      try (HttpClient client = new HttpClient(config.KAIROSDB_BASE_URL)) {
        long startTime = System.currentTimeMillis();
        int dataLoop = 200;
        for (int i = 0; i < dataLoop; i++) {
          System.out.println(String.format("%.2f", i * 100.0 / dataLoop) + "% data prepared");
          long recordTime = startTime + i * 200;
          for (String metric : config.METRIC_LIST.split(",")) {
            MetricBuilder builder = MetricBuilder.getInstance();
            builder.addMetric(metric)
                .addTag(Constants.TAG_KEY1, config.MACHINE_ID)
                .addDataPoint(recordTime, "666");
            client.pushMetrics(builder);
          }
        }
//        LOGGER.info();
        System.out.println("All test data prepared");
      } catch (IOException e) {
//        LOGGER.error("prepare kairosdb data failed", e);
        System.out.println("prepare kairosdb data failed");
        e.printStackTrace();
      }
    }

    if (!config.START_TIME.equals("") && !config.ENDED_TIME.equals("")) {
      startTime = TimeUtils.convertDateStrToTimestamp(config.START_TIME);
      endTime = TimeUtils.convertDateStrToTimestamp(config.ENDED_TIME);
      //LOGGER.info("开始查询KairosDB的数据...");
      System.out.println("开始查询KairosDB的数据...");
      long start = System.currentTimeMillis();
      long loadElapse = 0;
      long exportCsvElapse = 0;
      for (int i = 0; i < column_loop; i++) {
        if ((i + 1) * column > header.length) {
          metrics = Arrays.copyOfRange(header, i * column, header.length);
        } else {
          metrics = Arrays.copyOfRange(header, i * column, (i + 1) * column);
        }
        start = System.currentTimeMillis();
        loadAllMetricsOfOneTrain();
        loadElapse += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        exportDataTable(i);
        exportCsvElapse += System.currentTimeMillis() - start;
      }
      System.out.println("开始转换CSV文件为TsFile文件...");
      start = System.currentTimeMillis();
      tsFilePath = config.EXPORT_FILE_DIR + File.separator + String
          .format(TSFILE_FILE_NAME, config.MACHINE_ID, config.START_TIME, config.ENDED_TIME,
              header.length);
      TransToTsfile.transToTsfile(dirAbsolutePath, tsFilePath);
      long exportTsFileElapse = System.currentTimeMillis() - start;
      System.out.println(
          "查询KairosDB的数据耗时 " + loadElapse + " ms, " + "导出成CSV文件耗时 " + exportCsvElapse + " ms, "
              + "CSV转换为TsFile耗时 " + exportTsFileElapse + " ms");
//      for (File file : new File(dirAbsolutePath).listFiles()) {
//        file.delete();
//      }
//      new File(dirAbsolutePath).delete();
    } else {
      System.out.println("必须指定导出数据的起止时间！");
      //LOGGER.error("必须指定导出数据的起止时间！");
    }

    if (show) {
      try {
        readTsfile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }


  }

  private static void readTsfile() throws IOException {
    String filePath = tsFilePath;
    TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
    System.out.println("file length: " + new File(filePath).length());
    System.out.println("file magic head: " + reader.readHeadMagic());
    System.out.println("file magic tail: " + reader.readTailMagic());
    System.out.println("Level 1 metadata position: " + reader.getFileMetadataPos());
    System.out.println("Level 1 metadata size: " + reader.getFileMetadataSize());
    TsFileMetaData metaData = reader.readFileMetadata();
    // Sequential reading of one ChunkGroup now follows this order:
    // first SeriesChunks (headers and data) in one ChunkGroup, then the CHUNK_GROUP_FOOTER
    // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the marker) ahead and
    // judge accordingly.
    System.out.println("[Chunk Group]");
    System.out.println("position: " + reader.position());
    byte marker;
    while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
          System.out.println("\t[Chunk]");
          System.out.println("\tposition: " + reader.position());
          ChunkHeader header = reader.readChunkHeader();
          System.out.println("\tMeasurement: " + header.getMeasurementID());
          Decoder defaultTimeDecoder = Decoder.getDecoderByType(
              TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeEncoder),
              TSDataType.INT64);
          Decoder valueDecoder = Decoder
              .getDecoderByType(header.getEncodingType(), header.getDataType());
          for (int j = 0; j < header.getNumOfPages(); j++) {
            valueDecoder.reset();
            System.out.println("\t\t[Page]\n \t\tPage head position: " + reader.position());
            PageHeader pageHeader = reader.readPageHeader(header.getDataType());
            System.out.println("\t\tPage data position: " + reader.position());
            System.out.println("\t\tpoints in the page: " + pageHeader.getNumOfValues());
            ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
            System.out
                .println("\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
            PageReader reader1 = new PageReader(pageData, header.getDataType(), valueDecoder,
                defaultTimeDecoder);
            while (reader1.hasNextBatch()) {
              BatchData batchData = reader1.nextBatch();
              while (batchData.hasNext()) {
                System.out.println(
                    "\t\t\ttime, value: " + batchData.currentTime() + ", " + batchData
                        .currentValue());
                batchData.next();
              }
            }
          }
          break;
        case MetaMarker.CHUNK_GROUP_FOOTER:
          System.out.println("Chunk Group Footer position: " + reader.position());
          ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
          System.out.println("device: " + chunkGroupFooter.getDeviceID());
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    System.out.println("[Metadata]");
    List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata.getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        System.out.println(String
            .format("\t[Device]File Offset: %d, Device %s, Number of Chunk Groups %d",
                index.getOffset(), chunkGroupMetaData.getDeviceID(),
                chunkGroupMetaDataList.size()));

        for (ChunkMetaData chunkMetadata : chunkGroupMetaData.getChunkMetaDataList()) {
          System.out.println("\t\tMeasurement:" + chunkMetadata.getMeasurementUid());
          System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
        }
      }
    }
    reader.close();
  }


  private static void loadAllMetricsOfOneTrain() {
    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(new Date(startTime))
        .setEnd(new Date(endTime));
    for (String metric : metrics) {
      builder.addMetric(metric)
          .addTag(Constants.TAG_KEY1, trainNumber);
    }

    try {
      try {
        client = new HttpClient(config.KAIROSDB_BASE_URL);
      } catch (MalformedURLException e) {
        System.out.println("初始化KairosDB客户端失败");
        e.printStackTrace();
      }
      QueryResponse response = client.query(builder);

      for (QueryResult query : response.getQueries()) {
        for (Result result : query.getResults()) {
          if (result.getTags().get(Constants.TAG_KEY1) != null) {
            String machineId = result.getTags().get(Constants.TAG_KEY1).get(0);
            if (machineId != null) {
              String sensor = result.getName();

              for (DataPoint dp : result.getDataPoints()) {
                Map<String, Object> map = dataTable.get(dp.getTimestamp());
                if (map != null) {
                  map.put(sensor, dp.getValue());
                } else {
                  map = new HashMap<>();
                  map.put(sensor, dp.getValue());
                }
                dataTable.put(dp.getTimestamp(), map);
              }

            }
          } else {
            System.out
                .println("查询不到对应machine_id为 " + trainNumber + " 的数据，metric: " + result.getName());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    closeClient();
  }

  private static String getFullPath(String sensor) {
    String devicePath = "." + trainNumber;
    String group = getStorageGroupName(devicePath);
    return String.format(PATH_TEMPLATE, group, devicePath, sensor);
  }

  private static void exportDataTable(int i) {

    String csvFileName = dirAbsolutePath + File.separator + i + CSV_FILE_SUFFIX;

    File file = new File(csvFileName);

    //LOGGER.info("正在导出{}列, {}行数据到 {} ...", metrics.length, dataTable.size(), path);
    System.out
        .println(
            String.format("正在导出%d列, %d行数据到 %s ...", metrics.length, dataTable.size(), csvFileName));
    int count = 0;
    int stage = dataTable.size() / 20;
    try {
      try (FileWriter writer = new FileWriter(file)) {
        // 填写csv文件表头
        StringBuilder headBuilder = new StringBuilder();
        headBuilder.append("Time");
        for (String metric : metrics) {
          headBuilder.append(",").append(getFullPath(metric));
        }
        headBuilder.append("\n");
        writer.write(headBuilder.toString());

        //
        for (Map.Entry<Long, Map<String, Object>> entry : dataTable.entrySet()) {
          StringBuilder lineBuilder = new StringBuilder(entry.getKey() + "");
          Map<String, Object> record = entry.getValue();
          for (String metric : metrics) {
            Object value = record.get(metric);
            if (value != null) {
              lineBuilder.append(",").append(value);
            } else {
              lineBuilder.append(",");
            }
          }
          lineBuilder.append("\n");
          writer.write(lineBuilder.toString());
          count++;
          if (stage > 0) {
            if (count % stage == 0) {
              double a = count * 100.0 / dataTable.size();
              String p = String.format("%.1f", a);
              System.out.println("已完成 " + p + "%");
            }
          } else {
            double a = count * 100.0 / dataTable.size();
            String p = String.format("%.1f", a);
            System.out.println("已完成 " + p + "%");
          }
        }
      }
      dataTable = new LinkedHashMap<>();
    } catch (IOException e) {
//      LOGGER.error("KairosDB数据导出为CSV文件失败", e);
      System.out.println("KairosDB数据导出为CSV文件失败");
      e.printStackTrace();
    }
  }

  private static String getStorageGroupName(String path) {
    if (path == null) {
      return "null";
    }
    String device = path.split("\\.")[1];
    for (int i = 0; i < config.PROTOCAL_NUM; i++) {
      if (config.PROTOCAL_MACHINE.get(i).contains(device)) {
        return String.format("%s%s", STORAGE_GROUP_PREFIX, i);
      }
    }
    int hashCode = device.hashCode();
//    LOGGER.error("协议中不存在车辆{}", device);
    return String.format("%s%s", STORAGE_GROUP_PREFIX, Math.abs(hashCode) % storageGroupSize);
  }

  private static void closeClient() {
    try {
      client.close();
    } catch (IOException e) {
      //LOGGER.error("关闭KairosDB客户端失败", e);
      System.out.println("关闭KairosDB客户端失败");
      e.printStackTrace();
    }
  }

  public static String concatFiles(String dirPath) {
    File[] csvFiles = new File(dirPath).listFiles();
    String csvFileName = String
        .format(CSV_FILE_NAME, config.MACHINE_ID, config.START_TIME, config.ENDED_TIME,
            header.length);
    csvFileName = config.EXPORT_FILE_DIR + File.separator + csvFileName;
    ArrayList<BufferedReader> fileReaders = new ArrayList<>();
    for (File csvFile : csvFiles) {
      try {
        fileReaders.add(new BufferedReader(new FileReader(csvFile)));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
    String line = null;
    try (BufferedWriter finalCsv = new BufferedWriter(new FileWriter(new File(csvFileName)))) {
      while ((line = fileReaders.get(0).readLine()) != null) {
        finalCsv.write(line);
        if (fileReaders.size() > 1) {
          for (int i = 1; i < fileReaders.size(); i++) {
            finalCsv.write(",");
            finalCsv.write(fileReaders.get(i).readLine().substring(line.indexOf(",") + 1));
          }
        }
        finalCsv.write("\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return csvFileName;
  }

}
