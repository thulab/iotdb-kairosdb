package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import java.util.Map;

public class RollUpRecovery {

  public void recover(Map<String, RollUp> historyTasks) throws RollUpException{
    for(RollUp rollUp: historyTasks.values()){
      RollUpsExecutor.getInstance().create(rollUp);
    }
  }
}
