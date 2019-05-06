package cn.edu.tsinghua.it.delete;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.it.insert.AddPointsTest;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeletePointsTest {

  private static String url;

  @BeforeClass
  public static void before() {
    RestService restService = new RestService();
    url = restService.getUrlPrefix() + "/api/v1/datapoints/delete";
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }
  }

  @Test
  public void TestDeleteDataPoints() {
    new AddPointsTest().testAddDataPoint();

    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(204, response.code());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
