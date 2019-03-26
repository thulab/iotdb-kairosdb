package cn.edu.tsinghua.iotdb.kairosdb;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.sql.SQLException;

public class Main {

    private static String host;
    private static String port;
    private static String user;
    private static String password;
    private static String storageGroup;

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static URI getBaseURI() { return UriBuilder.fromUri("http://localhost/").port(6666).build(); }

    private static URI baseURI = getBaseURI();

    private static HttpServer startServer() throws SQLException, ClassNotFoundException, ConfigurationException {
        readProperties();
        initDB();
        final ResourceConfig rc = new ResourceConfig().packages("cn.edu.tsinghua.iotdb.kairosdb.http.rest");
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
    }

    private static void readProperties() throws ConfigurationException {
        PropertiesConfiguration pcfg = new PropertiesConfiguration("config/config.properties");
        host = pcfg.getString("database.host", "localhost");
        port = pcfg.getString("database.port", "6667");
        user = pcfg.getString("database.user", "root");
        password = pcfg.getString("database.password", "root");
        storageGroup = pcfg.getString("database.storageGroup", null);
    }

    private static void initDB() throws SQLException, ClassNotFoundException {
        LOGGER.info("Ready to connect to IoTDB.");
        IoTDBUtil.initConnection(host, port, user, password);
        LOGGER.info("Connected successfully.");
        MetricsManager.loadMetadata(storageGroup);
    }

    public static void main(String[] argv) throws Exception {
        HttpServer server = startServer();
        LOGGER.info("IoTDB REST server has been available at {}.", baseURI);
        Thread.currentThread().join();
        server.shutdown();
        IoTDBUtil.closeConnection();
    }

}
