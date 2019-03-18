package com.threathunter.mock.simulator;



import com.threathunter.mock.util.PropertyUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class OrderSubmitEventsAction extends CommonEventsAction {

  String ip;
  private Map<String, Object> changedProperties;

  public OrderSubmitEventsAction(String host, int mockCount) {
    super(host, mockCount);
    ip = PropertyUtil.getRandomIP();
    changedProperties = new HashMap<>();
  }

  public OrderSubmitEventsAction(String host, int mockCount, String ip) {
    super(host, mockCount);
    this.ip = ip;
    changedProperties = new HashMap<>();
  }


  @Override
  public void apply(EventsActionVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void constructEvents() {
    for (int i = 0; i < mockCount; i++) {
      EventBuilder eventBuilder = getConcreteEventBuilder();
      director(eventBuilder);
      eventBuilder.getProp().putAll(changedProperties);
      addEventsToAction(eventBuilder);
    }
  }


  /*@Override
  public void addEventsToAction(EventBuilder eventBuilder) {
    CompositeEvent compositeEvent = eventBuilder.getCompositeEvent();
    events.add(compositeEvent.getParent());
    events.add(compositeEvent.getChild());
  }*/

  @Override
  public EventBuilder getConcreteEventBuilder() {
    return new OrderSubmitEventBuilder(host, ip);
  }

  public Map<String, Object> getChangedProperties() {
    return changedProperties;
  }

  public void addChangedProperties(Map<String, Object> properties) {
    changedProperties.putAll(properties);
  }
}
