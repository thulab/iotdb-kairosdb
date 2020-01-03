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

  private List<List<Session>> sessionsList = new ArrayList<>();

  private IoTDBSessionPool() {
    createSessions();
  }

  public synchronized void createSessions() {

    sessionsList.clear();
    for (int j = 0; j < config.URL_LIST.size(); j++) {
      // for different IoTDB
      List<Session> sessions = new ArrayList<>();
      for (int i = 0; i < config.CONNECTION_NUM; i++) {
        // each IoTDB creates multiple sessions
        try {
          String url = config.URL_LIST.get(j);
          String host = url.split(":")[0];
          int port = Integer.parseInt(url.split(":")[1]);
          Session session = new Session(host, port, "root", "root");
          session.open();
          sessions.add(session);
        } catch (Exception e) {
          LOGGER.error("Get new session failed ", e);
        }
      }
      sessionsList.add(sessions);
    }
  }

  public List<Session> getSessions() {
    List<Session> sessions;
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
      loop.set(0);
    }
    sessions = new ArrayList<>();
    for (int i = 0; i < config.URL_LIST.size(); i++) {
      sessions.add(sessionsList.get(i)
          .get(loop.getAndIncrement() % config.CONNECTION_NUM));
    }
    return sessions;
  }

  private static class IoTDBSessionPoolHolder {
    private static final IoTDBSessionPool INSTANCE = new IoTDBSessionPool();
  }

  public static IoTDBSessionPool getInstance() {
    return IoTDBSessionPoolHolder.INSTANCE;
  }

}
