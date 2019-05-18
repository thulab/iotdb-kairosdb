package cn.edu.tsinghua.iotdb.kairosdb.dao;

import com.google.gson.stream.JsonReader;
import java.io.Reader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageQueue {

  private ConcurrentLinkedQueue<Reader> queue = new ConcurrentLinkedQueue<>();
  private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

  private static class MessageQueueHolder {

    private static final MessageQueue INSTANCE = new MessageQueue();
  }

  public static MessageQueue getInstance() {
    return MessageQueueHolder.INSTANCE;
  }

  public Reader poll() {
    return queue.poll();
  }

  public void add(Reader jsonReader) {
    queue.add(jsonReader);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

}
