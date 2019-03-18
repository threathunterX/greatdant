package com.threathunter.persistent.core.api;


import com.threathunter.persistent.core.filter.PropertyFilter;
import com.threathunter.persistent.core.io.QueryCSVWriter;

import java.util.Map;

/**
 * 
 */
public class LogsReadContextBuilder {

  private String eventName;
  private QueryCSVWriter writer;
  private Map<String, PropertyFilter> namedFilters;
  private Long startPoint;
  private Long endPoint;
  private Map<String, Object> queryProperties;

  public LogsReadContextBuilder(String eventName, Long startPoint, Long endPoint) {
    this.eventName = eventName;
    this.startPoint = startPoint;
    this.endPoint = endPoint;
  }

  public LogsReadContextBuilder writer(QueryCSVWriter writer) {
    this.writer = writer;
    return this;
  }

  public QueryCSVWriter getWriter() {
    return writer;
  }

  public void setWriter(QueryCSVWriter writer) {
    this.writer = writer;
  }

  public LogsReadContextBuilder filters(Map<String, PropertyFilter> namedFilters) {
    this.namedFilters = namedFilters;
    return this;
  }

  public Map<String, PropertyFilter> getNamedFilters() {
    return namedFilters;
  }

  public void setNamedFilters(
      Map<String, PropertyFilter> namedFilters) {
    this.namedFilters = namedFilters;
  }

  public LogsReadContextBuilder start(Long startPoint) {
    this.startPoint = startPoint;
    return this;
  }

  public Long getStartPoint() {
    return startPoint;
  }

  public void setStartPoint(Long startPoint) {
    this.startPoint = startPoint;
  }

  public LogsReadContextBuilder end(Long endPoint) {
    this.endPoint = endPoint;
    return this;
  }

  public Long getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(Long endPoint) {
    this.endPoint = endPoint;
  }

  public LogsReadContextBuilder properties(Map<String, Object> queryProperties) {
    this.queryProperties = queryProperties;
    return this;
  }

  public Map<String, Object> getQueryProperties() {
    return queryProperties;
  }

  public void setQueryProperties(Map<String, Object> queryProperties) {
    this.queryProperties = queryProperties;
  }

  public LogsReadContext build() {
    return new LogsReadContext(this);
  }

  public String getEventName() {
    return eventName;
  }

  public void setEventName(String name) {
    this.eventName = name;
  }

  public LogsReadContextBuilder eventName(String name) {
    this.eventName = name;
    return this;
  }
}
