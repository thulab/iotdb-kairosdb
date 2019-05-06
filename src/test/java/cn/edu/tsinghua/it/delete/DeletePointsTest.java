package cn.edu.tsinghua.it.delete;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeletePointsTest {

  private static RestService restService;

  @BeforeClass
  public static void before() {
    restService = new RestService();
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }
  }

  @Test
  public void TestDeleteDataPoints() {
    insert();
    query();
    delete();
    query2();
    delete2();
    query3();
  }

  private void insert() {
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

    try {
      Response response = new HttpUtil(restService.getInsertUrl()).post(data);
      int statusCode = response.code();
      assertEquals(204, statusCode);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void query() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\"gro"
        + "up_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\",\"ser"
        + "ver2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],[1400000001000,13.2"
        + "],[1400000002000,23.1],[1400000003000,24.0],[1400000004000,24.1],[1400000005000,24.2],["
        + "1400000006000,24.3],[1400000007000,24.4],[1400000008000,24.5],[1400000009000,24.6],[140"
        + "0000010000,24.7],[1400000011000,24.8],[1400000012000,24.9],[1400000013000,25.0],[140000"
        + "0014000,25.1],[1400000015000,25.2],[1400000016000,25.3],[1400000017000,25.4],[140000001"
        + "8000,25.5],[1400000019000,25.6],[1400000020000,25.7],[1400000021000,25.8],[140000002200"
        + "0,25.9],[1400000023000,26.0],[1400000024000,26.1],[1400000025000,26.2],[1400000026000,2"
        + "6.3],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(restService.getQueryUrl()).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void delete() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"tags\""
        + ":{\"host\":[\"server2\"]}}]}";

    try {
      Response response = new HttpUtil(restService.getDeleteUrl()).post(data);
      assertEquals(204, response.code());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void query2() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"tags\""
        + ":{\"host\":[\"server1\"]}}]}";

    String expect = "{\"queries\":[{\"sample_size\":19,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\""
        + "],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],[1400000001000,13.2],"
        + "[1400000002000,23.1],[1400000003000,24.0],[1400000004000,24.1],[1400000009000,24.6]"
        + ",[1400000010000,24.7],[1400000011000,24.8],[1400000012000,24.9],[1400000013000,25.0]"
        + ",[1400000014000,25.1],[1400000015000,25.2],[1400000016000,25.3],[1400000017000,25.4]"
        + ",[1400000023000,26.0],[1400000024000,26.1],[1400000025000,26.2],[1400000026000,26.3]"
        + ",[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(restService.getQueryUrl()).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void delete2() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    try {
      Response response = new HttpUtil(restService.getDeleteUrl()).post(data);
      assertEquals(204, response.code());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void query3() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    String expect = "{\"queries\":[{\"sample_size\":0,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[],\"tags\":{},\"values\":[]}]}]}";

    try {
      Response response = new HttpUtil(restService.getQueryUrl()).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
