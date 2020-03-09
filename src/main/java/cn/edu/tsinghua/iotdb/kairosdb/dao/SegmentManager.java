package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.ArrayList;
import java.util.List;

public class SegmentManager {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private Long startTime;
  private Long endTime;
  private long[] timeVertex;
  private List<List<List<DBWrapper>>> connections = new ArrayList<>();

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public long[] getTimeVertex() {
    return timeVertex;
  }

  public List<List<List<DBWrapper>>> getConnections() {
    return connections;
  }

  public SegmentManager(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    analyseTimeFilter();
  }

  public static int writeSchemaHashCode(String schemaKey, int timeSegmentIndex) {
    return Math
        .abs(schemaKey.hashCode() % config.IoTDB_READ_ONLY_LIST.get(timeSegmentIndex).size());
  }

  public static int readSchemaHashCode(String schemaKey, int timeSegmentIndex,
      int schemaSegmentIndex) {
    return Math.abs(schemaKey.hashCode() % config.IoTDB_READ_ONLY_LIST.get(timeSegmentIndex)
        .get(schemaSegmentIndex).size());
  }

  private void analyseTimeFilter() {
    int splitSize = config.TIME_DIMENSION_SPLIT.size();
    long[] timeSplit = new long[splitSize + 1];
    for (int m = 0; m < splitSize; m++) {
      timeSplit[m] = config.TIME_DIMENSION_SPLIT.get(m);
    }
    timeSplit[splitSize] = System.currentTimeMillis();
    int startZone = 0;
    for (int i = 0; i < timeSplit.length; i++) {
      if (this.startTime <= timeSplit[i]) {
        startZone = i;
        break;
      } else {
        startZone = i + 1;
      }
    }
    int endZone = 0;
    for (int j = timeSplit.length - 1; j >= 0; j--) {
      if (this.endTime >= timeSplit[j]) {
        endZone = j;
        break;
      } else {
        endZone = -1;
      }
    }
    timeVertex = new long[endZone - startZone + 1 + 2];
    if (startZone > endZone) {
      timeVertex[0] = this.startTime;
      timeVertex[1] = this.endTime;
      if (startZone == timeSplit.length) {
        // query latest data, use write&read IoTDB Instance
        List<List<DBWrapper>> writeReadConnections =
            ConnectionPool.getInstance().getWriteReadConnections();
        int lastTimeSegmentIndex = writeReadConnections.size() - 1;
        List<DBWrapper> latestWriteReadCons = writeReadConnections.get(lastTimeSegmentIndex);
        List<List<DBWrapper>> latestWriteReadConsList = new ArrayList<>();
        for (DBWrapper dbWrapper : latestWriteReadCons) {
          List<DBWrapper> list = new ArrayList<>();
          list.add(dbWrapper);
          latestWriteReadConsList.add(list);
        }
        connections.add(latestWriteReadConsList);
      } else {
        connections.add(ConnectionPool.getInstance().getReadConnections().get(startZone));
      }
    } else {
      int midIndex = 1;
      timeVertex[0] = this.startTime;
      timeVertex[timeVertex.length - 1] = this.endTime;
      for (int index = startZone; index <= endZone; index++) {
        timeVertex[midIndex] = timeSplit[index];
        midIndex++;
      }
      for (int zone = startZone; zone <= endZone + 1; zone++) {
        connections.add(ConnectionPool.getInstance().getReadConnections().get(zone));
      }
    }
  }
}
