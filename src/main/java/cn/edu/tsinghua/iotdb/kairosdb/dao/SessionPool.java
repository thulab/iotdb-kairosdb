package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPool.class);
  private AtomicInteger loop = new AtomicInteger(0);

  private List<List<List<DBWrapper>>> sessionsList = new ArrayList<>();

  private SessionPool() {
    createSessions();
  }

  public synchronized void createSessions() {

    sessionsList.clear();
    for (int timeSegmentIndex = 0; timeSegmentIndex < config.IoTDB_LIST.size();
        timeSegmentIndex++) {
      List<String> sameTimeSegmentUrlList = config.IoTDB_LIST.get(timeSegmentIndex);
      List<List<DBWrapper>> sameTimeSegmentSessionList = new ArrayList<>();
      for (String typeUrl : sameTimeSegmentUrlList) {
        List<DBWrapper> sessions = new ArrayList<>();
        for (int i = 0; i < config.CONNECTION_NUM; i++) {
          String dbType = typeUrl.split("=")[0];
          String url = typeUrl.split("=")[1];
          try {
            DBWrapper dbWrapper = new DBWrapper(dbType, true, url);
            sessions.add(dbWrapper);
          } catch (Exception e) {
            LOGGER.error("Get new DBWrapper of {} failed ", typeUrl, e);
          }
        }
        sameTimeSegmentSessionList.add(sessions);
      }
      sessionsList.add(sameTimeSegmentSessionList);
    }
  }

  public List<List<DBWrapper>> getSessions() {
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
      loop.set(0);
    }
    List<List<DBWrapper>> list = new ArrayList<>();
    for (int i = 0; i < config.IoTDB_LIST.size(); i++) {
      List<List<DBWrapper>> sameSegmentWriteRead = sessionsList.get(i);
      List<DBWrapper> sameSegmentWriteReadCurrent = new ArrayList<>();
      for (List<DBWrapper> sessions : sameSegmentWriteRead) {
        sameSegmentWriteReadCurrent
            .add(sessions.get(loop.getAndIncrement() % config.CONNECTION_NUM));
      }
      list.add(sameSegmentWriteReadCurrent);
    }
    return list;
  }

  private static class IoTDBSessionPoolHolder {
    private static final SessionPool INSTANCE = new SessionPool();
  }

  public static SessionPool getInstance() {
    return IoTDBSessionPoolHolder.INSTANCE;
  }

}
