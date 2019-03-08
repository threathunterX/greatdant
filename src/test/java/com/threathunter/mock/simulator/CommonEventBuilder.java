package com.threathunter.mock.simulator;

import com.threathunter.mock.util.PropertyUtil;
import com.threathunter.model.Event;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yy on 17-8-8.
 */
public abstract class CommonEventBuilder implements EventBuilder {

  protected final Map<String, Object> prop;
  protected Long ts;
  protected String host;
  protected String ip;

  public CommonEventBuilder(String host, String ip) {
    ts = System.currentTimeMillis();
    this.host = host;
    this.ip = ip;
    prop = new HashMap<>();
  }

  public CommonEventBuilder(String host) {
    this(host, PropertyUtil.getRandomIP());
  }

  public abstract void buildPropertyValues();

  public abstract void buildEventValues();

  public abstract void buildSpecificAttribute();


  public Event getEvent() {
    throw new UnsupportedOperationException("this event is not a single  Event");
  }

  public CompositeEvent getCompositeEvent() {
    throw new UnsupportedOperationException("this event is not a composite  Event");
  }

  public void resetTimeStamp() {
    this.ts = System.currentTimeMillis();
  }

  public Map<String, Object> getProp() {
    return prop;
  }

 /* @Override
  public EventBuilderType getEventType() {
    throw new UnsupportedOperationException("internal error");
  }*/
}
