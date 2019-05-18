package cn.edu.tsinghua.iotdb.kairosdb.dao;

import com.google.gson.stream.JsonReader;
import java.io.Reader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageQueue {

  private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
  private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

  private static class MessageQueueHolder {

    private static final MessageQueue INSTANCE = new MessageQueue();
  }

  public static MessageQueue getInstance() {
    return MessageQueueHolder.INSTANCE;
  }

  public String poll() {
    return queue.poll();
  }

  public void add(String json) {
    queue.add(json);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

}
