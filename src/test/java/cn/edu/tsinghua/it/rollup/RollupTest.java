package cn.edu.tsinghua.it.rollup;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpStoreImpl;
import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import okhttp3.Response;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class RollupTest {

  private static RestService restService = new RestService();
  private static String rollupJson = "{\"name\":\"MyRollup3\","
      + "\"execution_interval\":{\"value\":2,\"unit\":\"seconds\"},"
      + "\"rollups\":[{\"save_as\":\"rollup4\","
      + "\"query\":{\"cache_time\":0,"
      + "\"start_relative\":{\"value\":\"100\","
      + "\"unit\":\"minutes\"},"
      + "\"end_relative\":{\"value\":\"1\""
      + ",\"unit\":\"seconds\"},"
      + "\"metrics\":[{\"name\":\"kairosdb.jvm.free_memory\","
      + "\"tags\":{},"
      + "\"aggregators\":[{\"name\":\"sum\","
      + "\"sampling\":{\"value\":5,\"unit\":\"minutes\"}}]}]}}]}";

  private static String updatedRollupJson = "{\"name\":\"MyRollup2\","
      + "\"execution_interval\":{\"value\":3,\"unit\":\"seconds\"},"
      + "\"rollups\":[{\"save_as\":\"rollup4\","
      + "\"query\":{\"cache_time\":0,"
      + "\"start_relative\":{\"value\":\"50\","
      + "\"unit\":\"minutes\"},"
      + "\"end_relative\":{\"value\":\"1\""
      + ",\"unit\":\"seconds\"},"
      + "\"metrics\":[{\"name\":\"kairosdb.jvm.free_memory\","
      + "\"tags\":{},"
      + "\"aggregators\":[{\"name\":\"count\","
      + "\"sampling\":{\"value\":5,\"unit\":\"minutes\"}}]}]}}]}";

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

  @After
  public void after() throws SQLException, ClassNotFoundException {
    // clean up test data in root.SYSTEM.ROLLUP.json
    List<Connection> connections=IoTDBUtil.getConnection();
    try (Statement statement = IoTDBUtil.getConnection().get(0).createStatement()) {
      statement.execute("DELETE TIMESERIES root.SYSTEM.ROLLUP.json");
      statement
          .execute("CREATE TIMESERIES root.SYSTEM.ROLLUP.json WITH DATATYPE=TEXT, ENCODING=PLAIN");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDeleteRollupPersistence() {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      // create a new rollup task and get its id
      Response response = httpUtil.post(rollupJson);
      assert response.body() != null;
      String res = response.body().string();
      Map map = (Map) JSON.parse(res);
      String id = (String) map.get("id");

      //delete the task by update its data to "NULL" in IoTDB
      RollUpStoreImpl rollUpStore = new RollUpStoreImpl();
      rollUpStore.remove(id);

      try (Statement statement = IoTDBUtil.getConnection().get(0).createStatement()) {
        String sql = String.format("select json from root.SYSTEM.ROLLUP where time = %s", id);
        ResultSet rs = statement.executeQuery(sql);
        String json = null;
        while (rs.next()) {
          json = rs.getString(2);
        }
        assertEquals("NULL", json);
      } catch (Exception e) {
        e.printStackTrace();
      }
      int statusCode = response.code();
      assertEquals(200, statusCode);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDeleteRollup() {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      // create a new rollup task and get its id
      Response response = httpUtil.post(rollupJson);
      assert response.body() != null;
      String res = response.body().string();
      Map map = (Map) JSON.parse(res);
      String id = (String) map.get("id");

      //delete the task by update its data to "NULL" in IoTDB
      url += "/" + id;
      httpUtil = new HttpUtil(url);
      response = httpUtil.delete();

      try (Statement statement = IoTDBUtil.getConnection().get(0).createStatement()) {
        String sql = String.format("select json from root.SYSTEM.ROLLUP where time = %s", id);
        ResultSet rs = statement.executeQuery(sql);
        String json = null;
        while (rs.next()) {
          json = rs.getString(2);
        }
        assertEquals("NULL", json);
      } catch (Exception e) {
        e.printStackTrace();
      }
      int statusCode = response.code();
      assertEquals(204, statusCode);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String sendRollupTaskJson(String json) {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      Response response = httpUtil.post(json);
      assert response.body() != null;
      String res = response.body().string();
      Map map = (Map) JSON.parse(res);
      return (String) map.get("id");
    } catch (Exception e) {
      e.printStackTrace();
      return "";
    }
  }

  @Test
  public void testListRollup() {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      String id2 = sendRollupTaskJson(updatedRollupJson);
      Response response = httpUtil.get();
      assert response.body() != null;
      String res = response.body().string();
      String expected = "[{\"id\":\"" + id2 + "\"," + updatedRollupJson.substring(1) + "]";
      assertEquals(expected, res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetRollup() {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    try {
      String id1 = sendRollupTaskJson(rollupJson);
      String id2 = sendRollupTaskJson(updatedRollupJson);
      String url2 = url + "/" + id2;
      HttpUtil httpUtil = new HttpUtil(url2);
      Response response = httpUtil.get();
      assert response.body() != null;
      String res = response.body().string();
      String expected = "{\"id\":\"" + id2 + "\"," + updatedRollupJson.substring(1);
      assertEquals(expected, res);
      String url1 = url + "/" + id1;
      httpUtil = new HttpUtil(url1);
      response = httpUtil.get();
      assert response.body() != null;
      res = response.body().string();
      expected = "{\"id\":\"" + id1 + "\"," + rollupJson.substring(1);
      assertEquals(expected, res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUpdateRollupPersistence() {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      // create a new rollup task and get its id
      Response response = httpUtil.post(rollupJson);
      assert response.body() != null;
      String res = response.body().string();
      Map map = (Map) JSON.parse(res);
      String id = (String) map.get("id");

      //update the rollup task
      url += "/" + id;
      httpUtil = new HttpUtil(url);
      response = httpUtil.put(updatedRollupJson);
      assert response.body() != null;
      res = response.body().string();
      // System.out.println("res: " + res);
      String expected = String.format(
          "{\"id\":\"%s\",\"name\":\"MyRollup2\",\"attributes\":{\"url\":\"/api/v1/rollups/%s\"}}",
          id, id);
      assertEquals(expected, res);

      try (Statement statement = IoTDBUtil.getConnection().get(0).createStatement()) {
        String sql = String.format("select json from root.SYSTEM.ROLLUP where time = %s", id);
        ResultSet rs = statement.executeQuery(sql);
        String json = null;
        while (rs.next()) {
          json = rs.getString(2);
        }
        assertEquals(updatedRollupJson, json);
      } catch (Exception e) {
        e.printStackTrace();
      }
      int statusCode = response.code();
      assertEquals(200, statusCode);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCreateRollupPersistence() {
    String url = restService.getUrlPrefix() + "/api/v1/rollups";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      Response response = httpUtil.post(rollupJson);

      assert response.body() != null;
      String res = response.body().string();
      // System.out.println("res: " + res);
      Map map = (Map) JSON.parse(res);
      String id = (String) map.get("id");
      String expected = String.format(
          "{\"id\":\"%s\",\"name\":\"MyRollup3\",\"attributes\":{\"url\":\"/api/v1/rollups/%s\"}}",
          id, id);
      assertEquals(expected, res);

      try (Statement statement = IoTDBUtil.getConnection().get(0).createStatement()) {
        String sql = String.format("select json from root.SYSTEM.ROLLUP where time = %s", id);
        ResultSet rs = statement.executeQuery(sql);
        String json = null;
        while (rs.next()) {
          json = rs.getString(2);
        }
        assertEquals(rollupJson, json);
      } catch (Exception e) {
        e.printStackTrace();
      }
      int statusCode = response.code();
      assertEquals(200, statusCode);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
