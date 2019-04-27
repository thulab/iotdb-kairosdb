package cn.edu.tsinghua.it.query;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryByTagsTest {

  private static String url;

  @BeforeClass
  public static void before() {
    RestService restService = new RestService();
    url = restService.getUrlPrefix() + "/api/v1/datapoints/query";
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }
  }

  @Test
  public void queryWithoutTags() {
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
      Response response = new HttpUtil(url).post(data);
      assert response.code() == 200;
      assert response.body() != null;
      String result = response.body().string();
      assert expect.equals(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryWithTags() {
    String data = "{\"start_absolute\":1400000010000,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"tags\""
        + ":{\"host\":[\"server2\"]}}]}";

    String expect = "{\"queries\":[{\"sample_size\":5,\"results\":[{\"name\":\"test_query\",\"gro"
        + "up_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server2\"],\"d"
        + "ata_center\":[\"DC1\"]},\"values\":[[1400000018000,25.5],[1400000019000,25.6],[1400000"
        + "020000,25.7],[1400000021000,25.8],[1400000022000,25.9]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.code() == 200;
      assert response.body() != null;
      String result = response.body().string();
      assert expect.equals(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByAggregatorSum() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\"name"
        + "\":\"sum\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\"gro"
        + "up_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\",\"ser"
        + "ver2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],[1400000001000,36.3"
        + "],[1400000003000,48.1],[1400000005000,48.5],[1400000007000,48.9],[1400000009000,49.3],["
        + "1400000011000,49.7],[1400000013000,50.1],[1400000015000,50.5],[1400000017000,50.9],[140"
        + "0000019000,51.3],[1400000021000,51.7],[1400000023000,52.1],[1400000025000,52.5],[140000"
        + "0027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.code() == 200;
      assert response.body() != null;
      String result = response.body().string();
      assert expect.equals(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryBySumAlignSampling() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"sum\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"},\"align_sampling\":true"
        + "}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,25.5],[1400000002000"
        + ",47.1],[1400000004000,48.3],[1400000006000,48.7],[1400000008000,49.1],[1400000010000,"
        + "49.5],[1400000012000,49.9],[1400000014000,50.3],[1400000016000,50.7],[1400000018000,51.1]"
        + ",[1400000020000,51.5],[1400000022000,51.9],[1400000024000,52.3],[1400000026000,52.7]"
        + "]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.code() == 200;
      assert response.body() != null;
      String result = response.body().string();
      assert expect.equals(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryBySumAlignStartTime() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"sum\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"},\"align_start_time\":"
        + "true}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\",\""
        + "server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1399999998001,12.3],[1400000000001,"
        + "36.3],[1400000002001,48.1],[1400000004001,48.5],[1400000006001,48.9],[1400000008001,"
        + "49.3],[1400000010001,49.7],[1400000012001,50.1],[1400000014001,50.5],[1400000016001,"
        + "50.9],[1400000018001,51.3],[1400000020001,51.7],[1400000022001,52.1],[1400000024001,"
        + "52.5],[1400000026001,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.code() == 200;
      assert response.body() != null;
      String result = response.body().string();
      assert expect.equals(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryBySumAlignEndTime() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"sum\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"},\"align_end_time\":true"
        + "}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000001,12.3],[1400000002001"
        + ",36.3],[1400000004001,48.1],[1400000006001,48.5],[1400000008001,48.9],[1400000010001,"
        + "49.3],[1400000012001,49.7],[1400000014001,50.1],[1400000016001,50.5],[1400000018001,"
        + "50.9],[1400000020001,51.3],[1400000022001,51.7],[1400000024001,52.1],[1400000026001,"
        + "52.5],[1400000028001,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.code() == 200;
      assert response.body() != null;
      String result = response.body().string();
      assert expect.equals(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
