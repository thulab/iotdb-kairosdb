package kairosdb.export.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import kairosdb.export.csv.conf.CommandCli;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import kairosdb.export.csv.utils.TimeUtils;
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
  private static int storageGroupSize;
  private static final String PATH_TEMPLATE = "root.%s%s.%s";
  private static Map<Long, Map<String, Object>> dataTable = new LinkedHashMap<>();
  private static String[] metrics;
  private static String trainNumber;
  private static long startTime;
  private static long endTime;


  public static void main(String[] args) {

    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    storageGroupSize = config.STORAGE_GROUP_SIZE;
    trainNumber = config.MACHINE_ID;
    metrics = config.METRIC_LIST.split(",");

    boolean test = false;
    if (test) {
      try (HttpClient client = new HttpClient(config.KAIROSDB_BASE_URL)) {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 200; i++) {
          long recordTime = startTime + i * 1000;
          for (String metric : config.METRIC_LIST.split(",")) {
            MetricBuilder builder = MetricBuilder.getInstance();
            builder.addMetric(metric)
                .addTag(Constants.TAG_KEY1, config.MACHINE_ID)
                .addDataPoint(recordTime, "666");
            client.pushMetrics(builder);
          }
        }
//        LOGGER.info();
        System.out.println("data prepared");
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
      loadAllMetricsOfOneTrain();
      long loadElapse = System.currentTimeMillis() - start;
      start = System.currentTimeMillis();
      exportDataTable();
      long exportElapse = System.currentTimeMillis() - start;
      System.out.println(
          "查询KairosDB的数据耗时 " + loadElapse + " ms, " + "导出成CSV文件耗时 " + exportElapse + " ms");
    } else {
      System.out.println("必须指定导出数据的起止时间！");
      //LOGGER.error("必须指定导出数据的起止时间！");
    }


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

  private static void exportDataTable() {
    String template = "%s_%s_%s_%d.csv";
    String csvFileName = String
        .format(template, config.MACHINE_ID, config.START_TIME, config.ENDED_TIME, metrics.length);
    String path = config.EXPORT_FILE_DIR + File.separator + csvFileName;
    File file = new File(path);

    //LOGGER.info("正在导出{}列, {}行数据到 {} ...", metrics.length, dataTable.size(), path);
    System.out
        .println(String.format("正在导出%d列, %d行数据到 %s ...", metrics.length, dataTable.size(), path));
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
    int hashCode = path.hashCode();
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

}
