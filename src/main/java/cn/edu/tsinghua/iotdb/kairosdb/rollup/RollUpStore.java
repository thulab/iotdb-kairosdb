package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persistence rollup tasks by write JSON string into IoTDB
 */
public interface RollUpStore
{
	/**
	 * write rollup tasks to the store.
	 *
	 * @param rollUp rollup tasks to write to the store.
	 */
	void write(RollUp rollUp) throws RollUpException;

	/**
	 * Reads all tasks from the store
	 *
	 * @return all roll up tasks
	 */
	Map<String, RollUp> read() throws RollUpException;

	/**
	 Removes the task specified by the id.
	 @throws RollUpException if the task could not be removed
	 @param id id of the task to remove
	 */
	void remove(String id) throws RollUpException;

	/**
	 * Returns the task associated with the id.
	 *
	 * @param id task id
	 * @return task or null
	 */
	RollUp read(String id) throws RollUpException;

	/**
	 * Returns a list of all task ids
	 * @return list of task ids or an empty list
	 */
	Set<String> listIds()
			throws RollUpException;

	/**
	 * Returns the last time the store was modified.
	 * @return when the store was last modified
	 */
    long getLastModifiedTime()
			throws RollUpException;
}
