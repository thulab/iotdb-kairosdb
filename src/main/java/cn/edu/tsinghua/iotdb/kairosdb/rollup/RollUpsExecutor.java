package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollUpsExecutor {

  static final Logger LOGGER = LoggerFactory.getLogger(RollUpsExecutor.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private Map<String, ScheduledFuture> rollUpTasks = new HashMap<>();
  private ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) Executors
      .newScheduledThreadPool(config.MAX_ROLLUP);

  private static class RollUpsExecutorHolder {

    private static final RollUpsExecutor INSTANCE = new RollUpsExecutor();
  }

  public static RollUpsExecutor getInstance() {
    return RollUpsExecutorHolder.INSTANCE;
  }

  public void create(RollUp rollUp) throws RollUpException {
    if (rollUpTasks.size() <= config.MAX_ROLLUP) {
      long intervalValue = getIntervalValue(rollUp.getInterval());
      ScheduledFuture scheduledFuture = executor.scheduleAtFixedRate(rollUp, 1, intervalValue,
          TimeUnit.getUnitTimeInRollUp(rollUp.getInterval().getUnit()));
      rollUpTasks.put(rollUp.getId(), scheduledFuture);


    } else {
      throw new RollUpException("Rollup tasks pool has reached maximum capacity.");
    }

  }

  private long getIntervalValue(Duration duration) {
    switch (duration.getUnit()) {
      case WEEKS:
        return duration.getValue() * 7;
      case MONTHS:
        return duration.getValue() * 30;
      case YEARS:
        return duration.getValue() * 365;
      default:
        return duration.getValue();
    }
  }

  public void delete(String id) {
    System.out.println("RollUpsExecutor line 61: execute rollUpTasks.get(id).cancel(true)");
    rollUpTasks.get(id).cancel(true);
    rollUpTasks.remove(id);
  }

  public void update(RollUp rollUp) {

  }

}
