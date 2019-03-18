package com.threathunter.mock.simulator;

import com.threathunter.common.ObjectId;
import com.threathunter.mock.util.PropertyUtil;
import com.threathunter.model.Event;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class HttpDynamicEventBuilder extends CommonEventBuilder {

  private Event event;
  private String device;

  public HttpDynamicEventBuilder(String host) {
    super(host);
    event = new Event();
  }

  public HttpDynamicEventBuilder(String host, String ip) {
    super(host, ip);
    event = new Event();
  }

  public void buildPropertyValues() {
    Map<String, Object> prop = new HashMap<String, Object>();
    buildCommonHttpProperties(prop);
    buildSpiderHttpProperties(prop);
    getEvent().setPropertyValues(prop);
  }

  private void buildCommonHttpProperties(Map<String, Object> prop) {
    prop.put("referer", host);
    prop.put("c_port", 4430);
    prop.put("request_type", "click");
    prop.put("s_ip", "10.1.0.10");
    prop.put("s_body", PropertyUtil.getRandomStr(20));
    prop.put("useragent", "java-0.8.11");
    prop.put("platform", "PC");
    prop.put("sid", PropertyUtil.getRandomStr(10));
    prop.put("uid", "");
    prop.put("request_time", ts);
    prop.put("host", host);
    prop.put("s_port", 80);
    prop.put("method", "GET");
    prop.put("cookie", "");
    prop.put("c_body", PropertyUtil.getRandomStr(20));
    prop.put("did", getDevice());
    prop.put("referer_hit", "F");
    prop.put("status", 200);
  }

  private void buildSpiderHttpProperties(Map<String, Object> prop) {
    prop.put("s_type", "application/json");
    prop.put("c_ip", ip);
    prop.put("uri_stem", "/shop/item");
    prop.put("uri_query", "q=" + PropertyUtil.getRandomStr(5));
    prop.put("c_type", "application/json");
    prop.put("s_bytes", 23);
    prop.put("page", host + "/shop/item");
    prop.put("xforward", ip);
  }

  public void buildEventValues() {
    getEvent().setApp("nebula");
    getEvent().setId(new ObjectId().toHexString());
    getEvent().setKey(ip);
    getEvent().setName("HTTP_DYNAMIC");//HTTP_DYNAMIC：HTTP_CLICK/HTTP_API
    getEvent().setTimestamp(ts);
    getEvent().setValue(1.0);
  }

  public void buildSpecificAttribute() {

  }

  @Override
  public Event getEvent() {
    return this.event;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  @Override
  public EventBuilderType getEventType() {
    return EventBuilderType.LEAF;
  }
}
