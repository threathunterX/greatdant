package com.threathunter.mock.simulator;

import com.threathunter.mock.util.PropertyUtil;
import com.threathunter.model.Event;

/**
 * Created by yy on 17-8-7.
 */
public class HttpDynamicEventsAction extends CommonEventsAction {

  private String device;

  public HttpDynamicEventsAction(String host, int mockCount) {
    super(host, mockCount);
    device = PropertyUtil.getRandomDevice();
  }

  @Override
  public EventBuilder getConcreteEventBuilder() {
    HttpDynamicEventBuilder builder = new HttpDynamicEventBuilder(host);
    builder.setDevice(device);
    return builder;
  }

  @Override
  public void apply(EventsActionVisitor vistor) {
    vistor.visit(this);
  }

  @Override
  public void addEventsToAction(EventBuilder eventBuilder) {
    Event event = eventBuilder.getEvent();
    events.add(event);
    logger.debug("******add data. event is leaf. event id = {}. event description = {}", eventBuilder.getEvent().getId(), eventBuilder.getEvent());
  }

}
