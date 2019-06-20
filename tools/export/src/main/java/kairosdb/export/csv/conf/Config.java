package kairosdb.export.csv.conf;

public class Config {

  public String KAIROSDB_BASE_URL = "http://localhost:8080";
  public int STORAGE_GROUP_SIZE = 10;
  public String MACHINE_ID = "17";
  public String METRIC_LIST = "GZ4033,GZ4034,GZ4035";
  public String START_TIME = "2018-8-30T00:00:00+08:00";
  public String ENDED_TIME = "2018-8-30T00:10:00+08:00";
  public String EXPORT_FILE_DIR = "/home/ubuntu";

  Config() {
  }

}
