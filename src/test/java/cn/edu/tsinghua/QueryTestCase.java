package cn.edu.tsinghua;

import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import java.util.Date;
import okhttp3.Response;

public class QueryTestCase {

  private static final String URL = "http://127.0.0.1:8080/api/v1/datapoints";

  private static final String INSERT_JSON = "{\"name\":\"test_rollup\",\"datapoints\":[[%s,%s]],\"tags\":{\"host\":\"server1\",\"data_center\":\"DC1\"}}";

  private static Response raiseInsertOperation(String timestamp, String value) {
    try {
      return new HttpUtil(URL).post(String.format(INSERT_JSON, timestamp, value));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String[] argv) {

    for (int i = 30; true; i++) {
      raiseInsertOperation(String.valueOf(new Date().getTime()+100000L), String.valueOf(i));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

}
