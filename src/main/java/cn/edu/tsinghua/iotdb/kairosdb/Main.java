package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static URI getBaseURI() {
        return UriBuilder.fromUri("http://localhost/").port(6666).build();
    }

//    public static Config config = ConfigDescriptor.getInstance().getConfig();
    private static URI baseURI = getBaseURI();

    private static HttpServer startServer() throws SQLException, ClassNotFoundException {
        initIoTDB();
        final ResourceConfig rc = new ResourceConfig().packages("cn.edu.tsinghua.iotdb.kairosdb.http.rest");
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
    }

    private static void initIoTDB() throws SQLException, ClassNotFoundException {
        IoTDBUtil.initConnection("127.0.0.1", "6667", "root", "root");
    }

    public static void main(String[] argv) throws Exception {
        HttpServer server = startServer();
        LOGGER.info(String.format("IoTDB REST server has been available at %s.", baseURI));
        Thread.currentThread().join();
        server.shutdown();
        IoTDBUtil.closeConnection();
    }

}
