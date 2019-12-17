package cn.edu.tsinghua.iotdb.kairosdb.conf;

import java.util.ArrayList;
import java.util.List;

public class Config {

  public List<String> URL_LIST = new ArrayList<>();
  public int STORAGE_GROUP_SIZE = 50;
  public String REST_PORT = "6666";
  public String AGG_FUNCTION = "AVG";
  public String SPECIAL_TAG = "device";
  public int MAX_ROLLUP = 100;
  public int DEBUG = 0;
  public int CONNECTION_NUM = 20;

  public int GROUP_BY_UNIT = 20;
  public long MAX_RANGE = 20;
  public int PROFILE_INTERVAL = 10;
  public int CORE_POOL_SIZE = 0;
  public boolean ENABLE_PROFILER = false;

  public int PROTOCAL_NUM = 12;
  public List<List<String>> PROTOCAL_MACHINE = new ArrayList<>();


  Config() {

  }

}
