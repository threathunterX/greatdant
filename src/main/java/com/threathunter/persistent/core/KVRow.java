package com.threathunter.persistent.core;

import java.util.Map;

/**
 * 
 */
public class KVRow implements Comparable<KVRow> {

  private final Long timestamp;
  private final String name;
  private final Map<String, Object> showEntry;

  public KVRow(Long timestamp, String name, Map<String, Object> showEntry) {
    this.timestamp = timestamp;
    this.name = name;
    this.showEntry = showEntry;
  }

  @Override
  public int compareTo(KVRow entry) {
    if (getTimestamp() < entry.getTimestamp()) {
      return -1;
    } else if (getTimestamp() > entry.getTimestamp()) {
      return 1;
    }
    return getName().compareTo(entry.getName());
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getShowEntry() {
    return showEntry;
  }

  public Long getTimestamp() {
    return timestamp;
  }
}
