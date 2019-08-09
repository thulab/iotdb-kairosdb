/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.query.QueryMetric;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DeviceMetricTransposition {


  public void test() {

    Map<String, List<String>> deviceMetrics = new HashMap<>();

    String prefix = "root.*";

    QueryMetric m1 = new QueryMetric();
    m1.setName("m1");
    Map<String, List<String>> tags1 = new HashMap<>();
    tags1.put("t1", Arrays.asList("t1v1", "t1v2", "t1v3"));
    tags1.put("t2", Arrays.asList("t2v1", "t2v2"));
    m1.setTags(tags1);


    QueryMetric m2 = new QueryMetric();
    m1.setName("m2");
    Map<String, List<String>> tags2 = new HashMap<>();
    tags2.put("t1", Arrays.asList("t1v1", "t1v2", "t1v4"));
    tags2.put("t2", Arrays.asList("t2v1", "t2v3"));
    m2.setTags(tags2);


    Map<String, Integer> tag2pos = new HashMap<>();
    Map<Integer, String> pos2tag = new HashMap<>();

    tag2pos.put("t1", 0);
    tag2pos.put("t2", 2);
    tag2pos.put("t3", 1);

    pos2tag.put(0, "t1");
    pos2tag.put(1, "t3");
    pos2tag.put(2, "t2");


    for (QueryMetric metric : new QueryMetric[] {m1, m2}) {
      //拿到所有tag
      Map<String, List<String>> tagValues = metric.getTags();

      //判断该metric贡献的总的时间序列条数
      int totalDevices = 1;
      for (List<String> list : tagValues.values()) {
        totalDevices *= list.size();
      }

      //初始化出这些时序的前缀
      List<StringBuilder> devices = new ArrayList<>();
      for (int i = 0; i < totalDevices; i++) {
        devices.add(new StringBuilder(prefix));
      }

      //对每个位置进行构造
      for (int i = 0; i < tag2pos.size(); i++) {
        //看该位置是否有被指定了tag
        List<String> values = tagValues.get(pos2tag.get(i));
        if (values == null) {
          //若没有指定，则每个设备都加一个*
          for (StringBuilder d : devices) {
            d.append(".*");
          }
        } else {
          //若指定了，则将该tag进行乘法操作
          int repeat = totalDevices / values.size();
          int j = 0;
          for (String value : values) {
            for (int k = 0; k < repeat; k ++) {
              devices.get(j+k).append(".").append(value);
            }
            j += repeat;
          }
        }
      }
      for (StringBuilder d : devices) {
        String device = d.toString();
        if (deviceMetrics.containsKey(device)) {
          deviceMetrics.get(device).add(metric.getName());
        } else {
          List<String> m = new ArrayList<>();
          m.add(metric.getName());
          deviceMetrics.put(device, m);
        }
      }
    }

    for (Map.Entry<String, List<String>> entry : deviceMetrics.entrySet()) {
      System.out.println(entry.getKey() + " -> "  + Arrays.toString(entry.getValue().toArray()));
    }


  }


  @Test
  public void test2() {
    List<String>[] values = new List[3];
    values[0] = Arrays.asList("t1v1", "t1v2", "t1v3");
    values[1] = Arrays.asList("*");
    values[2] = Arrays.asList("t2v1", "t2v2");
    List<String> res = productPath(0, values);
    System.out.println(Arrays.toString(res.toArray()));
  }




  public List<String> productPath(int currentLevel, List<String>[] tagValuesInEachLevel) {
      if (currentLevel == tagValuesInEachLevel.length -1 ) {
        List<String> result = new ArrayList<>();
        for (String tagValue : tagValuesInEachLevel[tagValuesInEachLevel.length -1]) {
          result.add("." + tagValue);
        }
        return result ;
      } else {
        List<String> result = new ArrayList<>();
        List<String> subPaths = productPath(currentLevel + 1, tagValuesInEachLevel);
        for (String tagValue : tagValuesInEachLevel[currentLevel]) {
          for (String subPath : subPaths) {
            result.add("." + tagValue +  subPath);
          }
        }
        return result;
      }
  }

}
