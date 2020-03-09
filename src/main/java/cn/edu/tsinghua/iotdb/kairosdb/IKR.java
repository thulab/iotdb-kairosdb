package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.conf.Constants;
import cn.edu.tsinghua.iotdb.kairosdb.dao.ConnectionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.SessionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.util.AddressUtil;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IKR {

  private static final String USER = "root";
  private static final String PSW = "root";
  private static final Logger LOGGER = LoggerFactory.getLogger(IKR.class);
  private static Config config;
  private static URI baseURI;

  private static URI getBaseURI() {
    String restIp = AddressUtil.getLocalIpAddress();
    return UriBuilder.fromUri("http://" + restIp + "/").port(Integer.parseInt(config.REST_PORT))
        .build();
  }

  private static HttpServer startServer() throws SQLException, ClassNotFoundException {
    initDB();
    final ResourceConfig rc = new ResourceConfig()
        .packages("cn.edu.tsinghua.iotdb.kairosdb.http.rest");
    return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
  }

  private static void initDB() throws SQLException, ClassNotFoundException {
    for (List<String> urls : config.IoTDB_LIST) {
      for(String typeUrl: urls) {
        LOGGER.info("Ready to connect to DB {}", typeUrl);
        String type = typeUrl.split("=")[0];
        String url = typeUrl.split("=")[1];
        if(type.equals(Constants.DB_IOT)) {
          Connection connection = IoTDBUtil.getConnection(url, USER, PSW);
          LOGGER.info("Connected {} successfully.", url);
          MetricsManager.loadMetadata(connection);
        }
      }
    }
    // init connections
    LOGGER.info("Initializing DB connections ...");
    ConnectionPool.getInstance().getWriteReadConnections();
    SessionPool.getInstance().getSessions();
  }

  private static HttpServer startServer(String[] argv) throws SQLException, ClassNotFoundException {
    CommandCli cli = new CommandCli();
    //argv = new String[]{"-cf", "conf/config.properties"};
    if (!cli.init(argv)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    baseURI = getBaseURI();
    LOGGER.info("connection informations for IoTDB");
    for (List<String> urls : config.IoTDB_LIST) {
      for(String url: urls) {
        String[] urlSplit = url.split(":");
        LOGGER.info("host = {}, port = {}", urlSplit[0], urlSplit[1]);
      }
    }
    return startServer();
  }

  public static void main(String[] argv) {
    HttpServer server = null;
    try {
      server = startServer(argv);
    } catch (Exception e) {
      LOGGER.error("启动IKR服务失败，请检查是否启动了IoTDB服务以及相关配置参数是否正确", e);
      System.exit(1);
    }
    LOGGER.info("IoTDB REST server has been available at {}.", baseURI);
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      LOGGER.error("主线程出现异常", e);
      Thread.currentThread().interrupt();
    }
    server.shutdown();
    IoTDBUtil.closeConnection();
  }

}
