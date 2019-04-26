package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import java.util.Map;

/**
 * Persistence rollup tasks by write JSON string into IoTDB
 */
public interface RollUpStore {

  /**
   * write rollup tasks to the store.
   *
   * @param rollUpJson rollup tasks JSON to write to the store.
   */
  void write(String rollUpJson, String id) throws RollUpException;

  /**
   * Reads all tasks from the store
   *
   * @return all roll up tasks
   */
  Map<String, RollUp> read() throws RollUpException;

  /**
   * Removes the task specified by the id.
   *
   * @param id id of the task to remove
   * @throws RollUpException if the task could not be removed
   */
  void remove(String id) throws RollUpException;

  /**
   * Returns the task associated with the id.
   *
   * @param id task id
   * @return task or null
   */
  RollUp read(String id) throws RollUpException;

}
