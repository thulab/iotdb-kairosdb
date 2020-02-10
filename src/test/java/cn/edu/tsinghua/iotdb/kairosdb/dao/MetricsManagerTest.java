package cn.edu.tsinghua.iotdb.kairosdb.dao;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.joda.time.DateTime;
import org.junit.Test;

public class MetricsManagerTest {

  @Test
  public void addDatapoint() {
    String res;
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long lt = new Long(1400000008000L);
    Date date = new Date(lt);
    res = simpleDateFormat.format(date);
    System.out.println(res);

    String vertex = "2014-5-14T00:53:28+08:00";
    DateTime dateTime = new DateTime(vertex);
    System.out.println(dateTime.getMillis());
  }
}