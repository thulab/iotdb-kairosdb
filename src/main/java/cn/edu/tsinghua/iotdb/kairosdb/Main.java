package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.disruptor.StringEvent;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.disruptor.StringEventFactory;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.disruptor.StringEventHandler;
import cn.edu.tsinghua.iotdb.kairosdb.util.AddressUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.net.URI;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final String USER = "root";
  private static final String PSW = "root";
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static Config config;
  private static URI baseURI;
  public static Disruptor<StringEvent> disruptor;

  private static URI getBaseURI() {
    String restIp = AddressUtil.getLocalIpAddress();
    return UriBuilder.fromUri("http://" + restIp + "/").port(Integer.parseInt(config.REST_PORT))
        .build();
  }

  private static HttpServer startServer() throws SQLException, ClassNotFoundException {
    initDB();

//    int cores = Runtime.getRuntime().availableProcessors();
//    if (config.WRITE_THREAD_NUM > 0) {
//      cores = config.WRITE_THREAD_NUM;
//    }
//    ExecutorService executorService = Executors.newFixedThreadPool(cores);
    EventFactory<StringEvent> eventFactory = new StringEventFactory();
    // Specify the size of the ring buffer, must be power of 2.
    int ringBufferSize = 1024 * 1024;
    disruptor = new Disruptor<>(eventFactory, ringBufferSize, DaemonThreadFactory.INSTANCE);
    EventHandler<StringEvent> eventHandler = new StringEventHandler();
    disruptor.handleEventsWith(eventHandler);
    disruptor.start();

    final ResourceConfig rc = new ResourceConfig()
        .packages("cn.edu.tsinghua.iotdb.kairosdb.http.rest");
    return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
  }

  private static void initDB() throws SQLException, ClassNotFoundException {
    LOGGER.info("Ready to connect to IoTDB.");
    IoTDBUtil.initConnection(config.HOST, config.PORT, USER, PSW);
    LOGGER.info("Connected successfully.");
    MetricsManager.loadMetadata();
  }

  private static HttpServer startServer(String[] argv) throws SQLException, ClassNotFoundException {
    CommandCli cli = new CommandCli();
    if (!cli.init(argv)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    baseURI = getBaseURI();
    LOGGER.info("host = {}, port = {}", config.HOST, config.PORT);
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
    disruptor.shutdown();
  }

}
