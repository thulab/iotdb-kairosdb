package kairosdb.export.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportTsfileOneDay extends Thread {

  private Config config = ConfigDescriptor.getInstance().getConfig();
  private HttpClient client;
  private static final Logger LOGGER = LoggerFactory.getLogger(ExportTsfileOneDay.class);
  private long startTime;
  private long endTime;
  private List<String> trainList;
  private String[] header;
  private String[] metrics;
  private int column_loop;
  private CountDownLatch downLatch;

  public ExportTsfileOneDay(long startTime, long endTime, List<String> trainList,
      String[] header, int column_loop, CountDownLatch downLatch) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.trainList = trainList;
    this.header = header;
    this.column_loop = column_loop;
    this.downLatch = downLatch;
  }

  private void exportDataTable(String trainNum, Map<Long, Map<String, Object>> dataTable, int i) {

    if (!new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
        + startTime).exists()) {
      new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
          + startTime).mkdir();
    }

    String csvFileName =
        ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
            + startTime + File.separator + trainNum + Constants.FILE_NAME_SEP + i
            + Constants.CSV_FILE_SUFFIX;

    File file = new File(csvFileName);

    //LOGGER.info("正在导出{}列, {}行数据到 {} ...", metrics.length, dataTable.size(), path);
    LOGGER.info("正在导出{}列, {}行数据到 {} ...", metrics.length, dataTable.size(), csvFileName);
    int count = 0;
    int stage = dataTable.size() / 20;
    try {
      try (FileWriter writer = new FileWriter(file)) {
        // 填写csv文件表头
        StringBuilder headBuilder = new StringBuilder();
        headBuilder.append("Time");
        for (String metric : metrics) {
          headBuilder.append(",").append(getFullPath(trainNum, metric));
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
//              System.out.println("已完成 " + p + "%");
//              LOGGER.info("已完成 {}%", p);
            }
          } else {
            double a = count * 100.0 / dataTable.size();
            String p = String.format("%.1f", a);
//            System.out.println("已完成 " + p + "%");
//            LOGGER.info("已完成 {}%", p);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("KairosDB数据导出为CSV文件失败", e);
      e.printStackTrace();
    }
    LOGGER.info("导出{}完成", csvFileName);
  }


  private Map<Long, Map<String, Object>> loadAllMetricsOfOneTrain(String trainNumber) {
    Map<Long, Map<String, Object>> dataTable = new LinkedHashMap<>();

    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(new Date(startTime))
        .setEnd(new Date(endTime));
    for (String metric : metrics) {
      builder.addMetric(metric)
          .addTag(config.TAG_NAME, trainNumber);
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
          if (result.getTags().get(config.TAG_NAME) != null) {
            String machineId = result.getTags().get(config.TAG_NAME).get(0);
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
    return dataTable;
  }

  private void exportOneTrainCsv(String trainNum) {
    for (int i = 0; i < column_loop; i++) {
      if ((i + 1) * config.COLUMN > header.length) {
        metrics = Arrays.copyOfRange(header, i * config.COLUMN, header.length);
      } else {
        metrics = Arrays.copyOfRange(header, i * config.COLUMN, (i + 1) * config.COLUMN);
      }
      Map<Long, Map<String, Object>> dataTable = loadAllMetricsOfOneTrain(trainNum);

      exportDataTable(trainNum, dataTable, i);
    }
  }

  @Override
  public void run() {
    try {
      for (String train : trainList) {
        exportOneTrainCsv(train);
      }
      downLatch.countDown();
    } catch (Exception e) {
      LOGGER.error("发生异常", e);
    }
  }

  private void closeClient() {
    try {
      client.close();
    } catch (IOException e) {
      LOGGER.error("关闭KairosDB客户端失败", e);
    }
  }

  private static String getFullPath(String trainNumber, String sensor) {
    String devicePath = "." + trainNumber;
    String group = ExportToCsv.getStorageGroupName(devicePath);
    return String.format(Constants.PATH_TEMPLATE, group, devicePath, sensor);
  }
}
