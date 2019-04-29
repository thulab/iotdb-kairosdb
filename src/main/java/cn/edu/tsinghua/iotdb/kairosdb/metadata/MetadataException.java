package cn.edu.tsinghua.iotdb.kairosdb.metadata;

public class MetadataException extends Exception {

  public MetadataException() {
  }

  public MetadataException(String message) {
    super(message);
  }

  public MetadataException(Throwable cause) {
    super(cause);
  }

  public MetadataException(String message, Throwable cause) {
    super(message, cause);
  }

}
