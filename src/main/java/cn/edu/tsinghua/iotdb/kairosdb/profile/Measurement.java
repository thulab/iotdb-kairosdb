package cn.edu.tsinghua.iotdb.kairosdb.profile;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Measurement {

  private EnumMap<Profile, AtomicLong> profilerSum = new EnumMap<>(Profile.class);
  private EnumMap<Profile, AtomicLong> profilerCounter = new EnumMap<>(Profile.class);
  public static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);

  private Measurement() {
    for(Profile profile: Profile.values()) {
      profilerSum.put(profile, new AtomicLong(0));
      profilerCounter.put(profile, new AtomicLong(0));
    }
  }

  public void add(Profile item, long elapsedTime) {
    profilerSum.get(item).addAndGet(elapsedTime);
    profilerCounter.get(item).incrementAndGet();
  }

  public void show() {
    for(Profile profile: Profile.values()) {
      long sum = profilerSum.get(profile).get();
      double count = profilerCounter.get(profile).get();
      double avgTime = 0;
      if(count != 0) {
        avgTime = sum / 1000000.0 / count ;
      }
      LOGGER.info("[{}], average time cost: ,{}, ms", profile, avgTime);
    }
  }

  private static final class MeasurementHolder {
    private static final Measurement INSTANCE = new Measurement();
  }

  public static Measurement getInstance() {
    return MeasurementHolder.INSTANCE;
  }

  public enum Profile {
    FIRST_NEXT,
    IOTDB_QUERY,
    IKR_QUERY,
  }
}
