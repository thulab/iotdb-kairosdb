package cn.edu.tsinghua.iotdb.kairosdb.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

  private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
  public String HOST = "localhost";
  public String PORT = "6667";
  public int STORAGE_GROUP_SIZE = 50;

  Config() {

  }

}
