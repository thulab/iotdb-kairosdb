package kairosdb.export.csv;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kairosdb.export.csv.conf.CommandCli;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import kairosdb.export.csv.utils.TimeUtils;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportToCsv {

  private static Config config;
  private static final Logger LOGGER = LoggerFactory.getLogger(ExportToCsv.class);
  private static final String STORAGE_GROUP_PREFIX = "group_";
  private static final String DIR_NAME = "root.%s";

  private static final String TSFILE_FILE_NAME = "%s-%s-%s.tsfile";
  private static int storageGroupSize;
  private static List<String> trainList;
  private static long startTime;
  private static long endTime;
  private static String tsFilePath;
  private static int column;
  private static String[] header;
  public static String dirAbsolutePath;
  private static int dayNumber;


  public static void main(String[] args) {

    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    storageGroupSize = config.STORAGE_GROUP_SIZE;
    trainList = config.MACHINE_ID_LIST;
    column = config.COLUMN;
    header = config.METRIC_LIST.split(",");
    int column_loop = (int) Math.ceil((float) config.METRIC_LIST.split(",").length / column);
    for (String train : trainList) {
      String dirPath = String.format(DIR_NAME, getStorageGroupName("." + train));
      dirAbsolutePath = config.EXPORT_FILE_DIR + File.separator + dirPath;
      if (!new File(dirAbsolutePath).exists()) {
        new File(dirAbsolutePath).mkdir();
      }
    }
    if (!new File(dirAbsolutePath + File.separator + Constants.CSV_DIR).exists()) {
      new File(dirAbsolutePath + File.separator + Constants.CSV_DIR).mkdir();
    }

    boolean writeTest = false;
    if (writeTest) {
      try (HttpClient client = new HttpClient(config.KAIROSDB_BASE_URL)) {
        long startTime = System.currentTimeMillis();
        int dataLoop = 200;
        for (String machine : trainList) {
          for (int i = 0; i < dataLoop; i++) {
            long recordTime = startTime + i * 200;
            for (String metric : config.METRIC_LIST.split(",")) {
              MetricBuilder builder = MetricBuilder.getInstance();
              builder.addMetric(metric)
                  .addTag(config.TAG_NAME, machine)
                  .addDataPoint(recordTime, "666");
              client.pushMetrics(builder);
            }
          }
        }
        LOGGER.info("All test data prepared");
      } catch (IOException e) {
        LOGGER.error("prepare kairosdb data failed", e);
        e.printStackTrace();
      }
    }

    if (!config.START_TIME.equals("") && !config.ENDED_TIME.equals("")) {
      startTime = TimeUtils.convertDateStrToTimestamp(config.START_TIME);
      endTime = TimeUtils.convertDateStrToTimestamp(config.ENDED_TIME);
      dayNumber = TimeUtils.timeRange(startTime, endTime);
      CountDownLatch downLatch = new CountDownLatch(dayNumber);
      ExecutorService executorService = Executors.newFixedThreadPool(dayNumber);
      for (int i = 0; i < dayNumber; i++) {
        if (i == dayNumber - 1) {
          executorService.submit(
              new ExportTsfileOneDay(startTime + i * Constants.TIME_DAY, endTime, trainList, header,
                  column_loop, downLatch));
        } else {
          executorService.submit(
              new ExportTsfileOneDay(startTime + i * Constants.TIME_DAY,
                  startTime + (i + 1) * Constants.TIME_DAY, trainList, header,
                  column_loop, downLatch));
        }
      }
      executorService.shutdown();
      try {
        downLatch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOGGER.info("开始转换CSV文件为TsFile文件...");
      for (int i = 0; i < dayNumber; i++) {
        long tmpTime = startTime + i * Constants.TIME_DAY;
        tsFilePath =
            dirAbsolutePath + File.separator + Constants.SEQUENCE_DIR + File.separator + String
                .format(TSFILE_FILE_NAME, tmpTime, 0, 0);
        TransToTsfile.transToTsfile(
            dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + tmpTime,
            tsFilePath);
//        long exportTsFileElapse = System.currentTimeMillis() - start;
//        System.out.println(
//            "查询KairosDB的数据耗时 " + loadElapse + " ms, " + "导出成CSV文件耗时 " + exportCsvElapse + " ms, "
//                + "CSV转换为TsFile耗时 " + exportTsFileElapse + " ms");
        if (config.DELETE_CSV) {
          File[] files = new File(
              dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + tmpTime)
              .listFiles();
          for (File file : files) {
            file.delete();
          }
          new File(dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + tmpTime)
              .delete();
        }
      }
    } else {
//      System.out.println("必须指定导出数据的起止时间！");
      LOGGER.error("必须指定导出数据的起止时间！");
    }

  }

  public static String getStorageGroupName(String path) {
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
    LOGGER.warn("协议中不存在车辆{}", device);
    return String.format("%s%s", STORAGE_GROUP_PREFIX, Math.abs(hashCode) % storageGroupSize);
  }

}
