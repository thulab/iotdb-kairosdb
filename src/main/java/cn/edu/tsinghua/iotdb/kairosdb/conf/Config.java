package cn.edu.tsinghua.iotdb.kairosdb.conf;

import java.util.ArrayList;
import java.util.List;

public class Config {

  public List<List<String>> IoTDB_LIST = new ArrayList<>();
  public List<Long> TIME_DIMENSION_SPLIT = new ArrayList<>();
  public List<List<List<String>>> IoTDB_READ_ONLY_LIST = new ArrayList<>();

  public int STORAGE_GROUP_SIZE = 50;
  public long POINT_EDGE = 50000000;
  public long TIME_EDGE = 50000000;
  public String REST_PORT = "6666";
  public String AGG_FUNCTION = "AVG";
  public int MAX_ROLLUP = 100;
  public int DEBUG = 0;
  public int CONNECTION_NUM = 20;

  public int GROUP_BY_UNIT = 20;
  public long MAX_RANGE = 20;
  public long LOAD_PARAM_CYCLE = 1800000;
  public long LOAD_METADATA_CYCLE = 3600;
  public long LATEST_TIME_RANGE = 3600000;
  public int PROFILE_INTERVAL = 10;
  public int CORE_POOL_SIZE = 0;
  public int MAX_POOL_SIZE = 30;
  public boolean ENABLE_PROFILER = false;

  public int PROTOCAL_NUM = 12;
  public List<List<String>> PROTOCAL_MACHINE = new ArrayList<>();


  public Config() {

  }

}
