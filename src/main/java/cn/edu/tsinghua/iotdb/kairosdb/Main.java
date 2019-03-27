package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import java.net.URI;
import java.sql.SQLException;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final String USER = "root";
  private static final String PSW = "root";
  private static final String STORAGE_GROUP = "default";
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static Config config;
  private static URI baseURI = getBaseURI();

  private static URI getBaseURI() {
    return UriBuilder.fromUri("http://localhost/").port(6666).build();
  }

  private static HttpServer startServer() throws SQLException, ClassNotFoundException {
    initDB();
    final ResourceConfig rc = new ResourceConfig()
        .packages("cn.edu.tsinghua.iotdb.kairosdb.http.rest");
    return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
  }

  private static void initDB() throws SQLException, ClassNotFoundException {
    LOGGER.info("Ready to connect to IoTDB.");
    IoTDBUtil.initConnection(config.HOST, config.PORT, USER, PSW);
    LOGGER.info("Connected successfully.");
    MetricsManager.loadMetadata(STORAGE_GROUP);
  }

  public static void main(String[] argv) throws Exception {
    CommandCli cli = new CommandCli();
    if (!cli.init(argv)) {
      return;
    }
    config = ConfigDescriptor.getInstance().getConfig();

    LOGGER.info("host={},port={}", config.HOST, config.PORT);
    HttpServer server = startServer();
    LOGGER.info("IoTDB REST server has been available at {}.", baseURI);
    Thread.currentThread().join();
    server.shutdown();
    IoTDBUtil.closeConnection();
  }

}
