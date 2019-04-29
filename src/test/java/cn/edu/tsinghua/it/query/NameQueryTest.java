package cn.edu.tsinghua.it.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class NameQueryTest {

  private static RestService restService = new RestService();

  @BeforeClass
  public static void before() {
    // start restful service
    restService = new RestService();
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }

    // prepare test data
    String url = restService.getUrlPrefix() + "/api/v1/datapoints";

    HttpUtil httpUtil = new HttpUtil(url);
    try {
      String data1 =
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
      httpUtil.post(data1);
      String data2 =
          "[{\"name\":\"archive\",\"datapoints\":[[1359788500000,999],[1359788600000,3299.2],"
              + "[1359788710000,399.3]],\"tags\":{\"host\":\"server1\",\"data_center\":\"DC1\"}},"
              + "{\"name\":\"archive_file_search\",\"timestamp\":1359786400000,\"value\":3219.2,"
              + "\"tags\":{\"host\":\"server2\"}}]";
      httpUtil.post(data2);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testListMetricNames() {
    //http://[host]:[port]/api/v1/metricnames
    //http://[host]:[port]/api/v1/metricnames?prefix=[prefix]

    // case1: without prefix parameter
    String expected = "{\"results\":[\"archive_file_search\",\"test_query\",\"archive\"]}";
    testMetricNames("", expected);

    // case2: with prefix that contains metric
    expected = "{\"results\":[\"archive_file_search\",\"archive\"]}";
    testMetricNames("?prefix=ar", expected);

    // case3: with prefix that does not contain metric
    expected = "{\"results\":[]}";
    testMetricNames("?prefix=wd", expected);
  }

  private void testMetricNames(String suffix, String expected) {
    String url = restService.getUrlPrefix() + "/api/v1/metricnames" + suffix;
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      // get a list of all metric names
      Response response = httpUtil.get();
      assertNotNull(response.body());
      String res = response.body().string();
      assertEquals(expected, res);
      int statusCode = response.code();
      assertEquals(200, statusCode);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
