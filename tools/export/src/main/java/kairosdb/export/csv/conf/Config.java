package kairosdb.export.csv.conf;

import java.util.ArrayList;
import java.util.List;

public class Config {

  public String KAIROSDB_BASE_URL = "http://localhost:8080";
  public int STORAGE_GROUP_SIZE = 10;
  public int COLUMN = 100;
  public List<String> MACHINE_ID_LIST = new ArrayList<>();
  public String METRIC_LIST = "GZ4033,GZ4034,GZ4035";
  public String START_TIME = "2018-8-30T00:00:00+08:00";
  public String ENDED_TIME = "2018-8-30T00:10:00+08:00";
  public String EXPORT_FILE_DIR = "/home/ubuntu";
  public String TAG_NAME = "machine_id";
  public boolean DELETE_CSV = true;
  public int PROTOCAL_NUM = 12;
  public List<List<String>> PROTOCAL_MACHINE = new ArrayList<>();
  public int THREAD_NUM = 128;

  Config() {
  }

}
