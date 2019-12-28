package iotdb.export.csv;

import iotdb.export.csv.conf.CommandCli;
import iotdb.export.csv.conf.Config;
import iotdb.export.csv.conf.ConfigDescriptor;
import iotdb.export.csv.conf.Constants;
import iotdb.export.csv.utils.TimeUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportToCsvIoTDB {

  private static Config config;
  private static final Logger LOGGER = LoggerFactory.getLogger(ExportToCsvIoTDB.class);
  private static final String STORAGE_GROUP_PREFIX = "group_";
  private static final String DIR_NAME = "%s_%s_%s";
  private static final String CSV_FILE_SUFFIX = ".csv";
  private static int storageGroupSize;
  private static final String PATH_TEMPLATE = "root.%s%s.%s";
  private static Map<Long, Map<String, Object>> dataTable = new LinkedHashMap<>();
  private static String[] metrics;
  private static String trainNumber;
  private static long startTime;
  private static long endTime;
  private static int column;
  private static String[] header;
  private static String dirAbsolutePath;
  private static Connection connection;

  private static void initConnection() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      connection = DriverManager
          .getConnection(String.format(Constants.IOTDB_URL, config.IoTDB_URL), Constants.ROLE,
              Constants.ROLE);
    } catch (ClassNotFoundException | SQLException e) {
      LOGGER.error("建立连接异常", e);
    }
  }

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
    initConnection();

    if (!config.START_TIME.equals("") && !config.ENDED_TIME.equals("")) {
      startTime = TimeUtils.convertDateStrToTimestamp(config.START_TIME);
      endTime = TimeUtils.convertDateStrToTimestamp(config.ENDED_TIME);
      LOGGER.info("开始查询IoTDB的数据");
      long start;
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
      LOGGER.info("数据导出成功,查询IoTDB数据耗时:{}ms,导出成CSV文件耗时:{}ms", loadElapse, exportCsvElapse);
    } else {
      LOGGER.error("必须指定导出数据的起止时间！");
    }

  }

  private static void loadAllMetricsOfOneTrain() {
    StringBuilder queryString = new StringBuilder();
    queryString.append("select ");
    if (header.length == 1 && (header[0].equals("use_rawdata") || header[0]
        .equals("serial_number"))) {
      queryString.append(header[0]);
      queryString.append(" from root.*.*").append(config.MACHINE_ID).append(" where time>")
          .append(startTime).append(" and time<").append(endTime);
    } else {
      for (String metric : metrics) {
        queryString.append(metric).append(",");
      }
      queryString.deleteCharAt(queryString.length() - 1);
      queryString.append(" from root.*.").append(config.MACHINE_ID).append(" where time>")
          .append(startTime).append(" and time<").append(endTime);
    }

    try (Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(queryString.toString());
      if (hasResultSet) {
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
          Map<String, Object> map = new HashMap<>();
          for (int i = 2; i <= metrics.length + 1; i++) {
            map.put(metrics[i - 2], resultSet.getString(i));
          }
          dataTable.put(Long.parseLong(resultSet.getString(1)), map);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

  private static String getFullPath(String sensor) {
    String devicePath = "." + trainNumber;
    String group = getStorageGroupName(devicePath);
    return String.format(PATH_TEMPLATE, group, devicePath, sensor);
  }

  private static void exportDataTable(int i) {

    String csvFileName = dirAbsolutePath + File.separator + trainNumber + "-" + i + CSV_FILE_SUFFIX;

    File file = new File(csvFileName);

    LOGGER.info("正在导出{}列, {}行数据到 {} ...", metrics.length, dataTable.size(), csvFileName);
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
          StringBuilder lineBuilder = new StringBuilder(
              TimeUtils.timstamp2DateTime(entry.getKey()) + "");
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
              LOGGER.info("已完成 {}%", p);
            }
          } else {
            double a = count * 100.0 / dataTable.size();
            String p = String.format("%.1f", a);
            LOGGER.info("已完成 {}%", p);
          }
        }
      }
      dataTable = new LinkedHashMap<>();
    } catch (IOException e) {
      LOGGER.error("IoTDB数据导出为CSV文件失败", e);
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
//    LOGGER.warn("协议中不存在车辆{}", device);
    return String.format("%s%s", STORAGE_GROUP_PREFIX, Math.abs(hashCode) % storageGroupSize);
  }

}
