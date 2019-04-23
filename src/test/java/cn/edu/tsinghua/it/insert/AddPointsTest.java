package cn.edu.tsinghua.it.insert;

import cn.edu.tsinghua.iotdb.kairosdb.Main;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import java.net.ConnectException;
import okhttp3.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AddPointsTest {

  @Before
  public void before(){

    TestThread mainThread = new TestThread();
    mainThread.start();

    HttpUtil httpUtil = new HttpUtil("http://192.168.130.185:6666/myresource");
    while (true) {
      try {
        httpUtil.get();
        break;
      } catch (ConnectException ignored) {
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @After
  public void after(){
  }

  @Test
  public void addDataPoint() {
    String url = "http://192.168.130.185:6666/api/v1/datapoints";
    String data =
        "[{\"name\":\"test_query\",\"datapoints\":[[1400000000000,12.3],[1400000001000,13.2],"
            + "[1400000002000,23.1],[1400000003000,24],[1400000004000,24.1],[1400000009000,24."
            + "6],[1400000010000,24.7],[1400000011000,24.8],[1400000012000,24.9],[1400000013000"
            + ",25],[1400000014000,25.1],[1400000015000,25.2],[1400000016000,25.3],[140000001700"
            + "0,25.4],[1400000023000,26],[1400000024000,26.1],[1400000025000,26.2],[14000000260"
            + "00,26.3],[1400000027000,26.4]],\"tags\":{\"host\":\"server1\",\"data_center\":\"D"
            + "C1\"}},{\"name\":\"test_query\",\"datapoints\":[[1400000005000,24.2],[14000000060"
            + "00,24.3],[1400000007000,24.4],[1400000008000,24.5],[1400000018000,25.5],[14000000"
            + "19000,25.6],[1400000020000,25.7],[1400000021000,25.8],[1400000022000,25.9]],\"tag"
            + "s\":{\"host\":\"server2\",\"data_center\":\"DC1\"}}]";

    HttpUtil httpUtil = new HttpUtil(url);
    try {
      Response response = httpUtil.post(data);
      int statusCode = response.code();
      assert statusCode == 204;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private class TestThread extends Thread {
    @Override
    public void run() {
      String[] argv = {"-cf", "conf/config.properties"};
      try {
        Main.main(argv);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
