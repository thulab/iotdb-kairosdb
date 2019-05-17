package cn.edu.tsinghua.iotdb.kairosdb.dao;

import com.google.gson.stream.JsonReader;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageQueue {

  private ConcurrentLinkedQueue<JsonReader> queue = new ConcurrentLinkedQueue<>();

  private static class MessageQueueHolder {

    private static final MessageQueue INSTANCE = new MessageQueue();
  }

  public static MessageQueue getInstance() {
    return MessageQueueHolder.INSTANCE;
  }

  public JsonReader poll() {
    return queue.poll();
  }

  public void add(JsonReader jsonReader) {
    queue.add(jsonReader);
  }

}
