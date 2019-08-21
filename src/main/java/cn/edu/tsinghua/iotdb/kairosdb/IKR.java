package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.util.AddressUtil;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
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
    for (String url : config.URL_LIST) {
      LOGGER.info("Ready to connect to IoTDB. {}", url);
      Connection connection = IoTDBUtil.getConnection(url, USER, PSW);
      LOGGER.info("Connected {} successfully.", url);
      MetricsManager.loadMetadata(connection);
    }
  }

  private static HttpServer startServer(String[] argv) throws SQLException, ClassNotFoundException {
    CommandCli cli = new CommandCli();
    if (!cli.init(argv)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    baseURI = getBaseURI();
    LOGGER.info("connection informations for IoTDB");
    for (String url : config.URL_LIST) {
      LOGGER.info("host = {}, port = {}", url.split(":")[0], url.split(":")[1]);
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
