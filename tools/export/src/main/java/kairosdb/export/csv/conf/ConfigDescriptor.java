package kairosdb.export.csv.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigDescriptor {

  //private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);


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
        //LOGGER.warn("Fail to find config file {}", url);
        return;
      }
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
        config.KAIROSDB_BASE_URL = properties.getProperty("KAIROSDB_BASE_URL", config.KAIROSDB_BASE_URL);
        config.MACHINE_ID = properties.getProperty("MACHINE_ID", config.MACHINE_ID);
        config.METRIC_LIST = properties.getProperty("METRIC_LIST", config.METRIC_LIST);
        config.START_TIME = properties.getProperty("START_TIME", config.START_TIME);
        config.ENDED_TIME = properties.getProperty("ENDED_TIME", config.ENDED_TIME);
        config.EXPORT_FILE_DIR = properties.getProperty("EXPORT_FILE_DIR", config.EXPORT_FILE_DIR);
        config.STORAGE_GROUP_SIZE = Integer.parseInt(properties.getProperty("STORAGE_GROUP_SIZE", "10"));
      } catch (IOException e) {
        //LOGGER.error("load properties error: ", e);
      }
      try {
        inputStream.close();
      } catch (IOException e) {
        //LOGGER.error("Fail to close config file input stream", e);
      }
    } else {
      //LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
    }
  }

  private static class ConfigDescriptorHolder {
    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }
}
