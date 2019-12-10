package kairosdb.valid;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Test;

public class TimeTransfer {

  @Test
  public void dateToTimestamp() throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String time = "2018-09-29 16:39:00";
    Date date = format.parse(time);
    //日期转时间戳（毫秒）
    long timestamp = date.getTime();
    System.out.print("Format To times:" + timestamp);
  }

  @Test
  public void stampToDate() {
    String timestamp = "1538210340000";
    long lt = new Long(timestamp);
    Date date = new Date(lt);
    System.out.println("Format to Date" + date.toString());
  }
}
