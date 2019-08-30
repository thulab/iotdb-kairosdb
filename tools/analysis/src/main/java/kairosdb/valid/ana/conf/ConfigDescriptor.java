package kairosdb.valid.ana.conf;

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
    String url = System.getProperty(Constants.ANA_CONF, null);
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
        config.KAIROS_URL = properties.getProperty("KAIROS_URL", "http://127.0.0.1:8080");
        config.IKR_URL = properties.getProperty("IKR_URL","http://127.0.0.1:6666");

        String metricList = properties.getProperty("METRICS", "ZT1,ZT2");
        List<String> metrics = new ArrayList<>();
        Collections.addAll(metrics, metricList.split(","));
        config.METRICS = metrics;

        config.TAG = properties.getProperty("TAG", "1701");
        config.START_TIME_=properties.getProperty("START_TIME_");
        config.END_TIME_=properties.getProperty("END_TIME_");

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
