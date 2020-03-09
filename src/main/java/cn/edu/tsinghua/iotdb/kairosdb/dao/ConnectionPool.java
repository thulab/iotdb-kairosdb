package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);
  private AtomicInteger loop = new AtomicInteger(0);
  private AtomicInteger readOnlyLoop = new AtomicInteger(0);
  private List<List<List<DBWrapper>>> writeReadConnectionsList = new ArrayList<>();
  private List<List<List<List<DBWrapper>>>> readOnlyConnectionsList = new ArrayList<>();

  public List<List<List<DBWrapper>>> getReadConnections() {
    if (readOnlyLoop.incrementAndGet() > config.CONNECTION_NUM * 1000) {
      readOnlyLoop.set(0);
    }
    return readOnlyConnectionsList.get(readOnlyLoop.getAndIncrement() % config.CONNECTION_NUM);
  }

  private ConnectionPool() {
    createConnections();
  }

  public synchronized void createConnections() {
    writeReadConnectionsList.clear();
    for (int i = 0; i < config.CONNECTION_NUM; i++) {
      List<List<DBWrapper>> timeSchemaSegmentWriteReadCons = new ArrayList<>();
      for (int timeSegmentIndex = 0; timeSegmentIndex < config.IoTDB_LIST.size();
          timeSegmentIndex++) {
        List<String> sameTimeSegmentUrlList = config.IoTDB_LIST.get(timeSegmentIndex);
        createDBWrapper(timeSchemaSegmentWriteReadCons, sameTimeSegmentUrlList);
      }
      writeReadConnectionsList.add(timeSchemaSegmentWriteReadCons);
    }

    readOnlyConnectionsList.clear();
    for (int k = 0; k < config.CONNECTION_NUM; k++) {
      List<List<List<DBWrapper>>> sameTimeSegmentReadOnlyCons = new ArrayList<>();
      for (int timeSegmentIndex = 0; timeSegmentIndex < config.IoTDB_READ_ONLY_LIST.size();
          timeSegmentIndex++) {
        List<List<String>> sameTimeSegmentROUrlList = config.IoTDB_READ_ONLY_LIST.get(
            timeSegmentIndex);
        List<List<DBWrapper>> sameSchemaSegmentROCons = new ArrayList<>();
        for (List<String> sameSchemaSegmentROUrlList : sameTimeSegmentROUrlList) {
          createDBWrapper(sameSchemaSegmentROCons, sameSchemaSegmentROUrlList);
        }
        sameTimeSegmentReadOnlyCons.add(sameSchemaSegmentROCons);
      }
      readOnlyConnectionsList.add(sameTimeSegmentReadOnlyCons);
    }
  }

  private void createDBWrapper(List<List<DBWrapper>> sameSchemaSegmentROCons,
      List<String> sameSchemaSegmentROUrlList) {
    List<DBWrapper> sameInstanceCons = new ArrayList<>();
    for (String typeUrl : sameSchemaSegmentROUrlList) {
      String dbType = typeUrl.split("=")[0];
      String url = typeUrl.split("=")[1];
      try {
        DBWrapper dbWrapper = new DBWrapper(dbType, false, url);
        sameInstanceCons.add(dbWrapper);
      } catch (Exception e) {
        LOGGER.error("Get new DBWrapper of {} failed ", typeUrl, e);
      }
    }
    sameSchemaSegmentROCons.add(sameInstanceCons);
  }

  public List<List<DBWrapper>> getWriteReadConnections() {
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
      loop.set(0);
    }
    return writeReadConnectionsList.get(loop.getAndIncrement() % config.CONNECTION_NUM);
  }

  private static class IoTDBConnectionPoolHolder {
    private static final ConnectionPool INSTANCE = new ConnectionPool();
  }

  public static ConnectionPool getInstance() {
    return IoTDBConnectionPoolHolder.INSTANCE;
  }

}
