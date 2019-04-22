package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import java.util.Map;
import java.util.Set;

public class RollUpStoreImpl implements RollUpStore {


  @Override
  public void write(RollUp rollUp) throws RollUpException {

  }

  @Override
  public Map<String, RollUp> read() throws RollUpException {
    return null;
  }

  @Override
  public void remove(String id) throws RollUpException {

  }

  @Override
  public RollUp read(String id) throws RollUpException {
    return null;
  }

  @Override
  public Set<String> listIds() throws RollUpException {
    return null;
  }

  @Override
  public long getLastModifiedTime() throws RollUpException {
    return 0;
  }
}
