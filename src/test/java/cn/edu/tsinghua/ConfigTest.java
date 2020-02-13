package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.util.ReadFileUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.Test;

public class ConfigTest {

  private Config config;

  @Test
  public void addDatapoint() {
    config = new Config();
    String deploymentJsonStr = ReadFileUtils.readJsonFile("conf/DeploymentDescriptor.json");

    JSONArray jsonArray = JSON.parseArray(deploymentJsonStr);
    assert jsonArray != null;
    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject sameTimeSegment = (JSONObject) jsonArray.get(i);

      if (i > 0) {
        DateTime dateTime = new DateTime(sameTimeSegment.getString("scaleTime"));
        config.TIME_DIMENSION_SPLIT.add(dateTime.getMillis());
      }
      JSONArray schemaSplitArray = sameTimeSegment.getJSONArray("schemaSplit");
      List<String> writeReadUrlList = new ArrayList<>();
      List<List<String>> sameTimeSegmentReadOnlyUrlList = new ArrayList<>();
      for (int j = 0; j < schemaSplitArray.size(); j++) {
        JSONObject sameSchema = (JSONObject) schemaSplitArray.get(j);
        String writeReadUrl = sameSchema.getString("writeRead");
        writeReadUrlList.add(writeReadUrl);
        List<String> sameSchemaReadOnlyUrlList = new ArrayList<>();
        JSONArray readOnlyUrlArray = sameSchema.getJSONArray("readOnly");
        for (int k = 0; k < readOnlyUrlArray.size(); k++) {
          sameSchemaReadOnlyUrlList.add((String) readOnlyUrlArray.get(k));
        }
        sameTimeSegmentReadOnlyUrlList.add(sameSchemaReadOnlyUrlList);
      }
      config.IoTDB_LIST.add(writeReadUrlList);
      config.IoTDB_READ_ONLY_LIST.add(sameTimeSegmentReadOnlyUrlList);
    }

    System.out.println("finish");

  }
}
