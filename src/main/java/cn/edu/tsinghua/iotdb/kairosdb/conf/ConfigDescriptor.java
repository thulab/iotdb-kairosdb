package cn.edu.tsinghua.iotdb.kairosdb.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);

  private Config config;

  private ConfigDescriptor() {
    config = new Config();
    loadProps();
  }

  public static ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  public Config getConfig() {
    return config;
  }

  private void loadProps() {
    String url = System.getProperty(Constants.REST_CONF, null);
    if (url != null) {
      InputStream inputStream;
      try {
        inputStream = new FileInputStream(new File(url));
      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find config file {}", url);
        return;
      }
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
//        config.HOST = properties.getProperty("HOST", "127.0.0.1");
//        config.PORT = properties.getProperty("PORT", "6667");
        String urlList=properties.getProperty("IoTDB_LIST","127.0.0.1:6667");
        Collections.addAll(config.URL_LIST, urlList.split(","));
        config.REST_PORT = properties.getProperty("REST_PORT", "localhost");
        config.AGG_FUNCTION = properties.getProperty("AGG_FUNCTION", "AGG_FUNCTION");
        config.STORAGE_GROUP_SIZE = Integer.parseInt(properties.getProperty("STORAGE_GROUP_SIZE", "50"));
        config.MAX_ROLLUP = Integer.parseInt(properties.getProperty("MAX_ROLLUP", config.MAX_ROLLUP + ""));
        config.DEBUG = Integer.parseInt(properties.getProperty("DEBUG", config.DEBUG + ""));
        config.CONNECTION_NUM = Integer.parseInt(properties.getProperty("CONNECTION_NUM", config.CONNECTION_NUM + ""));
        config.GROUP_BY_UNIT = Integer.parseInt(properties.getProperty("GROUP_BY_UNIT", config.GROUP_BY_UNIT + ""));
        config.MAX_RANGE = Long.parseLong(properties.getProperty("MAX_RANGE", config.MAX_RANGE +
            ""));
        config.PROFILE_INTERVAL = Integer.parseInt(properties.getProperty("PROFILE_INTERVAL", config.PROFILE_INTERVAL + ""));
        config.ENABLE_PROFILER = Boolean.parseBoolean(properties.getProperty("ENABLE_PROFILER",
            config.ENABLE_PROFILER + ""));
      } catch (IOException e) {
        LOGGER.error("load properties error: ", e);
      }
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.error("Fail to close config file input stream", e);
      }
    } else {
      LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
    }
  }

  private static class ConfigDescriptorHolder {
    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }
}
