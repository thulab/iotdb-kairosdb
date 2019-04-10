package cn.edu.tsinghua.iotdb.kairosdb.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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

  public static final ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  public Config getConfig() {
    return config;
  }

  private void loadProps() {
    String url = System.getProperty(Constants.REST_CONF, null);
    if (url != null) {
      InputStream inputStream = null;
      try {
        inputStream = new FileInputStream(new File(url));
      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find config file {}", url);
        return;
      }
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
        config.HOST = properties.getProperty("HOST", "127.0.0.1");
        config.PORT = properties.getProperty("PORT", "6667");
        config.STORAGE_GROUP_SIZE = Integer.parseInt(properties.getProperty("STORAGE_GROUP_SIZE", "50"));
      } catch (IOException e) {
        e.printStackTrace();
      }
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOGGER.error("Fail to close config file input stream", e);
        }
      }
    } else {
      LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
    }
  }

  private static class ConfigDescriptorHolder {

    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }
}
