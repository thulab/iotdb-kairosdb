package cn.edu.tsinghua.iotdb.kairosdb.conf;

import java.util.ArrayList;
import java.util.List;

public class Config {

  //  public String HOST = "localhost";
//  public String PORT = "6667";
  public List<String> URL_LIST = new ArrayList<>();
  public int STORAGE_GROUP_SIZE = 20;
  public String REST_PORT = "6666";
  public int MAX_ROLLUP = 100;
  public int DEBUG = 0;
  public int CONNECTION_NUM = 20;
  public int GROUP_BY_UNIT = 20;

  Config() {

  }

}
