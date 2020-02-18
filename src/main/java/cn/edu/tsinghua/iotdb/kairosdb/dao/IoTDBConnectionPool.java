package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConnectionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectionPool.class);
  public static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  private AtomicInteger loop = new AtomicInteger(0);
  private AtomicInteger readOnlyLoop = new AtomicInteger(0);
  private List<List<List<Connection>>> writeReadConnectionsList = new ArrayList<>();
  private List<List<List<List<Connection>>>> readOnlyConnectionsList = new ArrayList<>();

  public List<List<List<Connection>>> getReadConnections() {
    if (readOnlyLoop.incrementAndGet() > config.CONNECTION_NUM * 1000) {
      readOnlyLoop.set(0);
    }
    return readOnlyConnectionsList.get(readOnlyLoop.getAndIncrement() % config.CONNECTION_NUM);
  }

  private IoTDBConnectionPool() {
    createConnections();
  }

  public synchronized void createConnections() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
    }
    writeReadConnectionsList.clear();
    for (int i = 0; i < config.CONNECTION_NUM; i++) {
      List<List<Connection>> timeSchemaSegmentWriteReadCons = new ArrayList<>();
      for (int timeSegmentIndex = 0; timeSegmentIndex < config.IoTDB_LIST.size();
          timeSegmentIndex++) {
        List<String> sameTimeSegmentUrlList = config.IoTDB_LIST.get(timeSegmentIndex);
        List<Connection> schemaSegmentConnections = new ArrayList<>();
        for (String url : sameTimeSegmentUrlList) {
          try {
            Connection con = DriverManager
                .getConnection(String.format(CONNECT_STRING, url), "root", "root");
            schemaSegmentConnections.add(con);
          } catch (SQLException e) {
            LOGGER.error("Get new connection failed ", e);
          }
        }
        timeSchemaSegmentWriteReadCons.add(schemaSegmentConnections);
      }
      writeReadConnectionsList.add(timeSchemaSegmentWriteReadCons);
    }

    readOnlyConnectionsList.clear();
    for (int k = 0; k < config.CONNECTION_NUM; k++) {
      List<List<List<Connection>>> sameTimeSegmentReadOnlyCons = new ArrayList<>();
      for (int timeSegmentIndex = 0; timeSegmentIndex < config.IoTDB_READ_ONLY_LIST.size();
          timeSegmentIndex++) {
        List<List<String>> sameTimeSegmentROUrlList = config.IoTDB_READ_ONLY_LIST.get(
            timeSegmentIndex);
        List<List<Connection>> sameSchemaSegmentROCons = new ArrayList<>();
        for (List<String> sameSchemaSegmentROUrlList : sameTimeSegmentROUrlList) {
          List<Connection> sameInstanceCons = new ArrayList<>();
          for (int j = 0; j < sameSchemaSegmentROUrlList.size(); j++) {
            try {
              Connection con = DriverManager
                  .getConnection(String.format(CONNECT_STRING, sameSchemaSegmentROUrlList.get(j)),
                      "root",
                      "root");
              sameInstanceCons.add(con);
            } catch (SQLException e) {
              LOGGER.error("Get new connection failed ", e);
            }
          }
          sameSchemaSegmentROCons.add(sameInstanceCons);
        }
        sameTimeSegmentReadOnlyCons.add(sameSchemaSegmentROCons);
      }
      readOnlyConnectionsList.add(sameTimeSegmentReadOnlyCons);
    }
  }

  public List<List<Connection>> getWriteReadConnections() {
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
      loop.set(0);
    }
    return writeReadConnectionsList.get(loop.getAndIncrement() % config.CONNECTION_NUM);
  }

  private static class IoTDBConnectionPoolHolder {
    private static final IoTDBConnectionPool INSTANCE = new IoTDBConnectionPool();
  }

  public static IoTDBConnectionPool getInstance() {
    return IoTDBConnectionPoolHolder.INSTANCE;
  }

}
