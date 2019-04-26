package cn.edu.tsinghua.it.query;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.Test;

public class QueryByTagsTest {

  private RestService mainThread = new RestService();
  private String url = mainThread.getUrlPrefix() + "/api/v1/datapoints/query";

  @Test
  public void queryWithoutTags() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.body() != null;
      String result = response.body().string();
      System.out.println(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryWithTags() {
    String data = "{\"start_absolute\":1400000010000,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"tags\""
        + ":{\"host\":[\"server2\"]}}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assert response.body() != null;
      String result = response.body().string();
      System.out.println(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }



}
