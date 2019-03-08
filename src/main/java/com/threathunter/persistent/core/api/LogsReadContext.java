package com.threathunter.persistent.core.api;

import com.threathunter.persistent.core.HourDirLogsRead;
import com.threathunter.persistent.core.KVRow;
import com.threathunter.persistent.core.LogsRead;
import com.threathunter.persistent.core.filter.PropertyFilter;
import com.threathunter.persistent.core.io.QueryCSVWriter;
import com.threathunter.persistent.core.util.PathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yy on 17-9-15.
 */
public class LogsReadContext {

  private static Logger logger = LoggerFactory.getLogger(LogsReadContext.class);
  private final String eventName;
  private QueryCSVWriter writer;
  private Map<String, PropertyFilter> namedFilters;
  private Long startPoint;
  private Long endPoint;
  private Map<String, Object> queryProperties;
  private List<String> dirs;
  private AtomicInteger total = new AtomicInteger(0);
  private AtomicInteger currentDir = new AtomicInteger(0);

  public LogsReadContext(String name, Long startPoint, Long endPoint,
                         Map<String, PropertyFilter> namedFilters, QueryCSVWriter writer) {
    this.eventName = name;
    this.startPoint = startPoint;
    this.endPoint = endPoint;
    this.namedFilters = namedFilters;
    dirs = PathHelper.getDirNamesFromTimestamp(startPoint, endPoint);
    this.writer = writer;
  }

  public LogsReadContext(LogsReadContextBuilder logsQueryContextBuilder) {
    this.eventName = logsQueryContextBuilder.getEventName();
    this.startPoint = logsQueryContextBuilder.getStartPoint();
    this.endPoint = logsQueryContextBuilder.getEndPoint();
    this.namedFilters = logsQueryContextBuilder.getNamedFilters();
    dirs = PathHelper.getDirNamesFromTimestamp(startPoint, endPoint);
    this.writer = logsQueryContextBuilder.getWriter();
    this.setQueryProperties(logsQueryContextBuilder.getQueryProperties());
  }

  public Map<String, PropertyFilter> getNamedFilters() {
    return namedFilters;
  }

  public void setNamedFilters(
      Map<String, PropertyFilter> namedFilters) {
    this.namedFilters = namedFilters;
  }

  public Long getStartPoint() {
    return startPoint;
  }

  public void setStartPoint(Long startPoint) {
    this.startPoint = startPoint;
  }

  public Long getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(Long endPoint) {
    this.endPoint = endPoint;
  }

  public void addrow(KVRow row) {
    if (writer == null) {
      throw new IllegalStateException("Query:writer:NUll,must initial writer first");
    }
//    System.out.println(String.format("%s,%s",row.getShowEntry().get("id"),row.getShowEntry().get("timestamp")));
    total.incrementAndGet();
    this.writer.writeQueryData(row.getShowEntry());
  }

  public void startQuery() {
    logger.info("query start in {}.", dirs);
    for (String dir : dirs) {
      if (PathHelper.ifDirExist(dir)) {
        LogsRead query = new HourDirLogsRead(dir);
        query.read(this);
      }
      currentDir.incrementAndGet();
    }
  }

  public void endQuery() {
    logger.info("query end.");
    writer.close();
  }

  public Double getProgress() {
    if(dirs.size()==0)
      return 0.0;
    return currentDir.get() * 1.0 / dirs.size();
  }

  public String getEventName() {
    return eventName;
  }

  public Map<String, Object> getQueryProperties() {
    return queryProperties;
  }

  public void setQueryProperties(Map<String, Object> queryProperties) {
    this.queryProperties = queryProperties;
  }

  public Map<String, Object> cloneQueryProperties() {
    Map<String, Object> copy = new HashMap<>();
    for (Map.Entry<String, Object> entry : queryProperties.entrySet()) {
      copy.put(entry.getKey(), entry.getValue());
    }
    return copy;
  }

  public Integer getTotalItem(){
    return total.get();
  }
}
