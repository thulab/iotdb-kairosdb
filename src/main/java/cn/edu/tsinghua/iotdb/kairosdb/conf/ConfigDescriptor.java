package cn.edu.tsinghua.iotdb.kairosdb.conf;

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
    new updateConfigThread().start();
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
        String urlList = properties.getProperty("IoTDB_LIST", "127.0.0.1:6667");
        List<String> urls = new ArrayList<>();
        Collections.addAll(urls, urlList.split(","));
        config.URL_LIST = urls;
        config.REST_PORT = properties.getProperty("REST_PORT", "localhost");
        config.PROTOCAL_NUM = Integer.parseInt(properties.getProperty("PROTOCAL_NUM", "12"));
        List<List<String>> protocal_machine = new ArrayList<>();
        for (int i = 1; i <= config.PROTOCAL_NUM; i++) {
          List<String> machines = new ArrayList<>();
          String machine_list = properties.getProperty("PROTOCAL_" + i, "");
          Collections.addAll(machines, machine_list.split(","));
          protocal_machine.add(machines);
        }
        config.PROTOCAL_MACHINE = protocal_machine;
        config.STORAGE_GROUP_SIZE = Integer
            .parseInt(properties.getProperty("STORAGE_GROUP_SIZE", "50"));
        config.MAX_ROLLUP = Integer
            .parseInt(properties.getProperty("MAX_ROLLUP", config.MAX_ROLLUP + ""));
        config.DEBUG = Integer.parseInt(properties.getProperty("DEBUG", config.DEBUG + ""));
        config.CONNECTION_NUM = Integer
            .parseInt(properties.getProperty("CONNECTION_NUM", config.CONNECTION_NUM + ""));
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


  private void updateProps() {
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
        config.PROTOCAL_NUM = Integer.parseInt(properties.getProperty("PROTOCAL_NUM", "12"));
        List<List<String>> protocal_machine = new ArrayList<>();
        for (int i = 1; i <= config.PROTOCAL_NUM; i++) {
          List<String> machines = new ArrayList<>();
          String machine_list = properties.getProperty("PROTOCAL_" + i, "");
          Collections.addAll(machines, machine_list.split(","));
          protocal_machine.add(machines);
        }
        config.PROTOCAL_MACHINE = protocal_machine;
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

  /**
   * 定时更新属性的线程
   */
  private class updateConfigThread extends Thread {

    //每30分钟更新一次config文件
    public void run() {
      while (true) {
        updateProps();
        try {
          LOGGER.info("定时更新了配置");
          Thread.sleep(1800000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

}
