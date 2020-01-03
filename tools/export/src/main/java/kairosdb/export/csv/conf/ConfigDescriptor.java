package kairosdb.export.csv.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        config.KAIROSDB_BASE_URL = properties
            .getProperty("KAIROSDB_BASE_URL", config.KAIROSDB_BASE_URL);
        String machine_id_list = properties.getProperty("MACHINE_ID_LIST", "1701");
        Collections.addAll(config.MACHINE_ID_LIST, machine_id_list.split(","));
        config.METRIC_LIST = properties.getProperty("METRIC_LIST", config.METRIC_LIST);
        config.START_TIME = properties.getProperty("START_TIME", config.START_TIME);
        config.ENDED_TIME = properties.getProperty("ENDED_TIME", config.ENDED_TIME);
        config.EXPORT_FILE_DIR = properties.getProperty("EXPORT_FILE_DIR", config.EXPORT_FILE_DIR);
        config.COLUMN = Integer.parseInt(properties.getProperty("COLUMN", "100"));
        config.STORAGE_GROUP_SIZE = Integer
            .parseInt(properties.getProperty("STORAGE_GROUP_SIZE", "10"));
        config.TAG_NAME = properties.getProperty("TAG_NAME", config.TAG_NAME);
        config.DELETE_CSV = Boolean.parseBoolean(properties.getProperty("DELETE_CSV", "true"));
        config.PROTOCAL_NUM = Integer.parseInt(properties.getProperty("PROTOCAL_NUM", "12"));
        List<List<String>> protocal_machine = new ArrayList<>();
        for (int i = 1; i <= config.PROTOCAL_NUM; i++) {
          List<String> machines = new ArrayList<>();
          String machine_list = properties.getProperty("PROTOCAL_" + i, "");
          Collections.addAll(machines, machine_list.split(","));
          protocal_machine.add(machines);
        }
        config.PROTOCAL_MACHINE = protocal_machine;
        config.THREAD_NUM = Integer.parseInt(properties.getProperty("THREAD_NUM", "128"));
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
