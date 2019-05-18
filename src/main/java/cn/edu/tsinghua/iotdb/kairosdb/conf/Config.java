package cn.edu.tsinghua.iotdb.kairosdb.conf;

public class Config {

  public String HOST = "localhost";
  public String PORT = "6667";
  public int STORAGE_GROUP_SIZE = 20;
  public String REST_PORT = "6666";
  public int MAX_ROLLUP = 100;
  public int WRITE_THREAD_NUM = 0;

  Config() {

  }

}
