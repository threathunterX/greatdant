package com.threathunter.mock.simulator;


import java.util.HashMap;
import java.util.Map;

import static com.threathunter.mock.util.PropertyUtil.*;


/**
 * 
 */
public class RegistrationRobotEventsAction extends CommonEventsAction {

  Map<String, Object> customProps;
  private String ip;
  private String sid;
  private String did;

  public RegistrationRobotEventsAction(String host, int mockCount) {
    super(host, mockCount);
    ip = getRandomIP();
    ip = getRandomIP();
    sid = getRandomStr(15);
    did = getRandomDevice();
    customProps = new HashMap<>();
    init();
  }

  private void init() {
    customProps.put("sid", sid);
    customProps.put("c_ip", ip);
    customProps.put("xforward", ip);
    customProps.put("did", did);
  }

  @Override
  public void apply(EventsActionVisitor vistor) {
    vistor.visit(this);
  }

  @Override
  public EventBuilder getConcreteEventBuilder() {
    return new RegistrationEventBuilder(host, ip);
  }

  public void constructEvents() {
    for (int i = 0; i < mockCount; i++) {
      EventBuilder eventBuilder = getConcreteEventBuilder();
      director(eventBuilder);
      eventBuilder.getProp().putAll(customProps);
      addEventsToAction(eventBuilder);
      threadSleepFor1s();
    }
  }


}
