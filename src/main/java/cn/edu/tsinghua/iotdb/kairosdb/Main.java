package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.sql.SQLException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static URI getBaseURI() { return UriBuilder.fromUri("http://localhost/").port(6666).build(); }

    private static URI baseURI = getBaseURI();

    private static HttpServer startServer() throws SQLException, ClassNotFoundException {
        initDB();
        final ResourceConfig rc = new ResourceConfig().packages("cn.edu.tsinghua.iotdb.kairosdb.http.rest");
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
    }

    private static void initDB() throws SQLException, ClassNotFoundException {
        IoTDBUtil.initConnection("127.0.0.1", "6667", "root", "root");
        MetricsManager.loadMetadata();
    }

    public static void main(String[] argv) throws Exception {
        HttpServer server = startServer();
        LOGGER.info(String.format("IoTDB REST server has been available at %s.", baseURI));
        Thread.currentThread().join();
        server.shutdown();
        IoTDBUtil.closeConnection();
    }

}
