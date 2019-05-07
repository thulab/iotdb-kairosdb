package cn.edu.tsinghua.it.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryTest {

  private static String url;
  private static final boolean isPrinting = false;

  private static RestService restService;

  @BeforeClass
  public static void before() {
    restService = new RestService();
    url = restService.getQueryUrl();
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }

    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    try {
      Response response = new HttpUtil(restService.getDeleteUrl()).post(data);
      assertEquals(204, response.code());
    } catch (IOException e) {
      e.printStackTrace();
    }

    data =
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

  @AfterClass
  public static void after() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":"
        + "\"days\"},\"time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\"}]}";

    try {
      Response response = new HttpUtil(restService.getDeleteUrl()).post(data);
      assertEquals(204, response.code());
    } catch (IOException e) {
      e.printStackTrace();
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
      if (isPrinting) {
        if (response.body() != null) {
          System.out.println(response.body().string());
        }
      } else {
        assertEquals(200, response.code());
        assertNotNull(response.body());
        String result = response.body().string();
        assertEquals(expect, result);
      }
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
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryBySum() {
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
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
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
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
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
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
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
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByAvg() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"tags\":{\"host\":["
        + "\"server2\"]},\"aggregators\":[{\"name\":\"avg\",\"sampling\":{\"value\":2,\"unit\":\""
        + "seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":9,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server2\"],"
        + "\"data_center\":[\"DC1\"]},\"values\":[[1400000005000,24.25],[1400000007000,24.45],"
        + "[1400000018000,25.5],[1400000019000,25.65],[1400000021000,25.85]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryBySumAndAvg() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"sum\",\"sampling\":{\"value\":\"2\",\"unit\":\"seconds\"}},{\"name\":\"avg\","
        + "\"sampling\":{\"value\":\"4\",\"unit\":\"seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\""
        + ",\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],[1400000001000"
        + ",42.2],[1400000005000,48.7],[1400000009000,49.5],[1400000013000,50.3],[1400000017000,"
        + "51.099999999999994],[1400000021000,51.900000000000006],[1400000025000,39.45]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByDev() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"dev\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"},\"return_type\":\"%s"
        + "\"}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,0.0],[1400000001000,"
        + "7.000357133746822],[1400000003000,0.07071067811865576],[1400000005000,"
        + "0.07071067811865576],[1400000007000,0.07071067811865576],[1400000009000,"
        + "0.07071067811865325],[1400000011000,0.07071067811865325],[1400000013000,"
        + "0.07071067811865576],[1400000015000,0.07071067811865576],[1400000017000,"
        + "0.07071067811865576],[1400000019000,0.07071067811865325],[1400000021000,"
        + "0.07071067811865325],[1400000023000,0.07071067811865576],[1400000025000,"
        + "0.07071067811865576],[1400000027000,0.0]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(String.format(data, "value"));
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }

    String expect2 = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],[1400000001000"
        + ",25.15035713374682],[1400000003000,24.120710678118655],[1400000005000,"
        + "24.320710678118655],[1400000007000,24.520710678118654],[1400000009000,"
        + "24.720710678118653],[1400000011000,24.920710678118656],[1400000013000,"
        + "25.120710678118655],[1400000015000,25.320710678118655],[1400000017000,"
        + "25.520710678118654],[1400000019000,25.720710678118653],[1400000021000,"
        + "25.920710678118656],[1400000023000,26.120710678118655],[1400000025000,"
        + "26.320710678118655],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(String.format(data, "pos_sd"));
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect2, result);
    } catch (IOException e) {
      e.printStackTrace();
    }

    String expect3 = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],"
        + "[1400000001000,11.149642866253178],[1400000003000,23.979289321881346],"
        + "[1400000005000,24.179289321881345],[1400000007000,24.379289321881345],"
        + "[1400000009000,24.579289321881344],[1400000011000,24.779289321881347],"
        + "[1400000013000,24.979289321881346],[1400000015000,25.179289321881345],"
        + "[1400000017000,25.379289321881345],[1400000019000,25.579289321881344],"
        + "[1400000021000,25.779289321881347],[1400000023000,25.979289321881346],"
        + "[1400000025000,26.179289321881345],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(String.format(data, "neg_sd"));
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect3, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByDiff() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"diff\"}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000001000,0.8999999999999986]"
        + ",[1400000002000,9.900000000000002],[1400000003000,0.8999999999999986],[1400000004000,"
        + "0.10000000000000142],[1400000005000,0.09999999999999787],[1400000006000,"
        + "0.10000000000000142],[1400000007000,0.09999999999999787],[1400000008000,"
        + "0.10000000000000142],[1400000009000,0.10000000000000142],[1400000010000,"
        + "0.09999999999999787],[1400000011000,0.10000000000000142],[1400000012000,"
        + "0.09999999999999787],[1400000013000,0.10000000000000142],[1400000014000,"
        + "0.10000000000000142],[1400000015000,0.09999999999999787],[1400000016000,"
        + "0.10000000000000142],[1400000017000,0.09999999999999787],[1400000018000,"
        + "0.10000000000000142],[1400000019000,0.10000000000000142],[1400000020000,"
        + "0.09999999999999787],[1400000021000,0.10000000000000142],[1400000022000,"
        + "0.09999999999999787],[1400000023000,0.10000000000000142],[1400000024000,"
        + "0.10000000000000142],[1400000025000,0.09999999999999787],[1400000026000,"
        + "0.10000000000000142],[1400000027000,0.09999999999999787]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByFirst() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\"name"
        + "\":\"first\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\""
        + ",\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],"
        + "[1400000001000,13.2],[1400000003000,24.0],[1400000005000,24.2],[1400000007000,24.4],"
        + "[1400000009000,24.6],[1400000011000,24.8],[1400000013000,25.0],[1400000015000,25.2],"
        + "[1400000017000,25.4],[1400000019000,25.6],[1400000021000,25.8],[1400000023000,26.0],"
        + "[1400000025000,26.2],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByLast() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\"name"
        + "\":\"last\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\""
        + ",\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],"
        + "[1400000002000,23.1],[1400000004000,24.1],[1400000006000,24.3],[1400000008000,24.5],"
        + "[1400000010000,24.7],[1400000012000,24.9],[1400000014000,25.1],[1400000016000,25.3],"
        + "[1400000018000,25.5],[1400000020000,25.7],[1400000022000,25.9],[1400000024000,26.1],"
        + "[1400000026000,26.3],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByMax() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\"name"
        + "\":\"max\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\""
        + ",\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],"
        + "[1400000002000,23.1],[1400000004000,24.1],[1400000006000,24.3],[1400000008000,24.5],"
        + "[1400000010000,24.7],[1400000012000,24.9],[1400000014000,25.1],[1400000016000,25.3],"
        + "[1400000018000,25.5],[1400000020000,25.7],[1400000022000,25.9],[1400000024000,26.1],"
        + "[1400000026000,26.3],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryByMin() {
    String data = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\"ti"
        + "me_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\"name"
        + "\":\"min\",\"sampling\":{\"value\":2,\"unit\":\"seconds\"}}]}]}";

    String expect = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\""
        + ",\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],"
        + "[1400000001000,13.2],[1400000003000,24.0],[1400000005000,24.2],[1400000007000,24.4],"
        + "[1400000009000,24.6],[1400000011000,24.8],[1400000013000,25.0],[1400000015000,25.2],"
        + "[1400000017000,25.4],[1400000019000,25.6],[1400000021000,25.8],[1400000023000,26.0],"
        + "[1400000025000,26.2],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(data);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryBySaveAs() {
    String query1 = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"test_query\",\"aggregators\":[{\""
        + "name\":\"save_as\",\"metric_name\":\"save_as_test\"}]}]}";

    String expect1 = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"test_query\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"host\":[\"server1\","
        + "\"server2\"],\"data_center\":[\"DC1\"]},\"values\":[[1400000000000,12.3],[1400000001000,"
        + "13.2],[1400000002000,23.1],[1400000003000,24.0],[1400000004000,24.1],[1400000005000,"
        + "24.2],[1400000006000,24.3],[1400000007000,24.4],[1400000008000,24.5],[1400000009000,"
        + "24.6],[1400000010000,24.7],[1400000011000,24.8],[1400000012000,24.9],[1400000013000,"
        + "25.0],[1400000014000,25.1],[1400000015000,25.2],[1400000016000,25.3],[1400000017000,"
        + "25.4],[1400000018000,25.5],[1400000019000,25.6],[1400000020000,25.7],[1400000021000,"
        + "25.8],[1400000022000,25.9],[1400000023000,26.0],[1400000024000,26.1],[1400000025000,"
        + "26.2],[1400000026000,26.3],[1400000027000,26.4]]}]}]}";

    String query2 = "{\"start_absolute\":1,\"end_relative\":{\"value\":\"5\",\"unit\":\"days\"},\""
        + "time_zone\":\"Asia/Kabul\",\"metrics\":[{\"name\":\"save_as_test\"}]}";

    String expect2 = "{\"queries\":[{\"sample_size\":28,\"results\":[{\"name\":\"save_as_test\",\""
        + "group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"saved_from\":[\""
        + "test_query\"]},\"values\":[[1400000000000,12.3],[1400000001000,13.2],[1400000002000,"
        + "23.1],[1400000003000,24.0],[1400000004000,24.1],[1400000005000,24.2],[1400000006000,"
        + "24.3],[1400000007000,24.4],[1400000008000,24.5],[1400000009000,24.6],[1400000010000,"
        + "24.7],[1400000011000,24.8],[1400000012000,24.9],[1400000013000,25.0],[1400000014000,"
        + "25.1],[1400000015000,25.2],[1400000016000,25.3],[1400000017000,25.4],[1400000018000,"
        + "25.5],[1400000019000,25.6],[1400000020000,25.7],[1400000021000,25.8],[1400000022000,"
        + "25.9],[1400000023000,26.0],[1400000024000,26.1],[1400000025000,26.2],[1400000026000,"
        + "26.3],[1400000027000,26.4]]}]}]}";

    try {
      Response response = new HttpUtil(url).post(query1);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      String result = response.body().string();
      assertEquals(expect1, result);

      response = new HttpUtil(url).post(query2);
      assertEquals(200, response.code());
      assertNotNull(response.body());
      result = response.body().string();
      assertEquals(expect2, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
