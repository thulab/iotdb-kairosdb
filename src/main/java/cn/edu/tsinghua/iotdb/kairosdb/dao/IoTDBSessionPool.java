package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSessionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSessionPool.class);
  private AtomicInteger loop = new AtomicInteger(0);

  private List<List<List<Session>>> sessionsList = new ArrayList<>();

  private IoTDBSessionPool() {
    createSessions();
  }

  public synchronized void createSessions() {

    sessionsList.clear();
    for (int timeSegmentIndex = 0; timeSegmentIndex < config.IoTDB_LIST.size();
        timeSegmentIndex++) {
      List<String> sameTimeSegmentUrlList = config.IoTDB_LIST.get(timeSegmentIndex);
      List<List<Session>> sameTimeSegmentSessionList = new ArrayList<>();
      for (String url : sameTimeSegmentUrlList) {
        List<Session> sessions = new ArrayList<>();
        for (int i = 0; i < config.CONNECTION_NUM; i++) {
          // each IoTDB creates multiple sessions
          try {
            String host = url.split(":")[0];
            int port = Integer.parseInt(url.split(":")[1]);
            Session session = new Session(host, port, "root", "root");
            session.open();
            sessions.add(session);
          } catch (Exception e) {
            LOGGER.error("Get new session failed ", e);
          }
        }
        sameTimeSegmentSessionList.add(sessions);
      }
      sessionsList.add(sameTimeSegmentSessionList);
    }
  }

  public List<List<Session>> getSessions() {
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
      loop.set(0);
    }
    List<List<Session>> list = new ArrayList<>();
    for (int i = 0; i < config.IoTDB_LIST.size(); i++) {
      List<List<Session>> sameSegmentWriteRead = sessionsList.get(i);
      List<Session> sameSegmentWriteReadCurrent = new ArrayList<>();
      for (List<Session> sessions : sameSegmentWriteRead) {
        sameSegmentWriteReadCurrent
            .add(sessions.get(loop.getAndIncrement() % config.CONNECTION_NUM));
      }
      list.add(sameSegmentWriteReadCurrent);
    }
    return list;
  }

  private static class IoTDBSessionPoolHolder {
    private static final IoTDBSessionPool INSTANCE = new IoTDBSessionPool();
  }

  public static IoTDBSessionPool getInstance() {
    return IoTDBSessionPoolHolder.INSTANCE;
  }

}
