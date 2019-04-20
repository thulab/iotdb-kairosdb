package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public enum GroupByKind {

  TYPE,
  TAGS,
  TIME,
  VALUE,
  BIN;

  public static GroupByKind fromString(String typeStr) {
    checkNotNullOrEmpty(typeStr);
    for (GroupByKind type : values()) {
      if (type.toString().equalsIgnoreCase(typeStr)) {
        return type;
      }
    }

    throw new IllegalArgumentException("No enum constant for " + typeStr);
  }

}
