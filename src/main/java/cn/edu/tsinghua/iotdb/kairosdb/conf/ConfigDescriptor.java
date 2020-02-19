package cn.edu.tsinghua.iotdb.kairosdb.conf;

import cn.edu.tsinghua.iotdb.kairosdb.util.ReadFileUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.joda.time.DateTime;
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

  private void loadDeploymentInfo() {
    // TODO:
    //  optimize and refactor:
    //  initialize the connection and session pools in this function, so that we can remove
    //  the IoTDB_LIST and IoTDB_READ_ONLY_LIST
    String deploymentJsonStr = ReadFileUtils.readJsonFile("conf/DeploymentDescriptor.json");
    try {
      JSONArray jsonArray = JSON.parseArray(deploymentJsonStr);
      assert jsonArray != null;
      for (int i = 0; i < jsonArray.size(); i++) {
        JSONObject sameTimeSegment = (JSONObject) jsonArray.get(i);

        if (i > 0) {
          DateTime dateTime = new DateTime(sameTimeSegment.getString("scaleTime"));
          config.TIME_DIMENSION_SPLIT.add(dateTime.getMillis());
        }
        JSONArray schemaSplitArray = sameTimeSegment.getJSONArray("schemaSplit");
        List<String> writeReadUrlList = new ArrayList<>();
        List<List<String>> sameTimeSegmentReadOnlyUrlList = new ArrayList<>();
        for (Object o : schemaSplitArray) {
          JSONObject sameSchema = (JSONObject) o;
          String writeReadUrl = sameSchema.getString("writeRead");
          writeReadUrlList.add(writeReadUrl);
          List<String> sameSchemaReadOnlyUrlList = new ArrayList<>();
          sameSchemaReadOnlyUrlList.add(writeReadUrl);
          JSONArray readOnlyUrlArray = sameSchema.getJSONArray("readOnly");
          for (Object value : readOnlyUrlArray) {
            sameSchemaReadOnlyUrlList.add((String) value);
          }
          sameTimeSegmentReadOnlyUrlList.add(sameSchemaReadOnlyUrlList);
        }
        config.IoTDB_LIST.add(writeReadUrlList);
        config.IoTDB_READ_ONLY_LIST.add(sameTimeSegmentReadOnlyUrlList);
      }
    } catch (Exception e) {
      LOGGER.error("Load deployment information failed! Please check the DeploymentDescriptor"
              + ".json file"
          , e);
    }
  }

  private void loadProps() {
    String url = System.getProperty(Constants.REST_CONF, null);
    loadDeploymentInfo();
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

        config.REST_PORT = properties.getProperty("REST_PORT", "localhost");
        config.AGG_FUNCTION = properties.getProperty("AGG_FUNCTION", "AGG_FUNCTION");
        config.STORAGE_GROUP_SIZE = Integer
            .parseInt(properties.getProperty("STORAGE_GROUP_SIZE", "50"));
        config.POINT_EDGE = Long.parseLong(properties.getProperty("POINT_EDGE", "50000000"));
        config.TIME_EDGE = Long.parseLong(properties.getProperty("TIME_EDGE", "50000000"));
        config.MAX_ROLLUP = Integer
            .parseInt(properties.getProperty("MAX_ROLLUP", config.MAX_ROLLUP + ""));
        config.DEBUG = Integer.parseInt(properties.getProperty("DEBUG", config.DEBUG + ""));
        config.CONNECTION_NUM = Integer
            .parseInt(properties.getProperty("CONNECTION_NUM", config.CONNECTION_NUM + ""));
        config.GROUP_BY_UNIT = Integer
            .parseInt(properties.getProperty("GROUP_BY_UNIT", config.GROUP_BY_UNIT + ""));
        config.MAX_RANGE = Long
            .parseLong(properties.getProperty("MAX_RANGE", config.MAX_RANGE + ""));
        config.LATEST_TIME_RANGE = Long
            .parseLong(properties.getProperty("LATEST_TIME_RANGE", config.LATEST_TIME_RANGE + ""));
        config.PROFILE_INTERVAL = Integer
            .parseInt(properties.getProperty("PROFILE_INTERVAL", config.PROFILE_INTERVAL + ""));
        config.CORE_POOL_SIZE = Integer
            .parseInt(properties.getProperty("CORE_POOL_SIZE", config.CORE_POOL_SIZE + ""));
        config.MAX_POOL_SIZE = Integer
            .parseInt(properties.getProperty("MAX_POOL_SIZE", config.MAX_POOL_SIZE + ""));
        config.ENABLE_PROFILER = Boolean.parseBoolean(properties.getProperty("ENABLE_PROFILER",
            config.ENABLE_PROFILER + ""));

        config.PROTOCAL_NUM = Integer.parseInt(properties.getProperty("PROTOCAL_NUM", "12"));
        List<List<String>> protocolMachine = new ArrayList<>();
        for (int i = 1; i <= config.PROTOCAL_NUM; i++) {
          List<String> machines = new ArrayList<>();
          String machineList = properties.getProperty("PROTOCAL_" + i, "");
          Collections.addAll(machines, machineList.split(","));
          protocolMachine.add(machines);
        }
        config.PROTOCAL_MACHINE = protocolMachine;
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
        List<List<String>> protocolMachine = new ArrayList<>();
        for (int i = 1; i <= config.PROTOCAL_NUM; i++) {
          List<String> machines = new ArrayList<>();
          String machineList = properties.getProperty("PROTOCAL_" + i, "");
          Collections.addAll(machines, machineList.split(","));
          protocolMachine.add(machines);
        }
        config.PROTOCAL_MACHINE = protocolMachine;
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
