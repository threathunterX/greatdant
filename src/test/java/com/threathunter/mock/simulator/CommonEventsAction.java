package com.threathunter.mock.simulator;

import com.threathunter.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.threathunter.mock.util.PropertyUtil.collect;

/**
 * Created by yy on 17-8-8.
 */
public abstract class CommonEventsAction implements EventsAction {

  protected final int mockCount;
  protected final String host;
  protected List<Event> events;
  Logger logger = LoggerFactory.getLogger(CommonEventsAction.class);

  public CommonEventsAction(String host, int mockCount) {
    events = new LinkedList<Event>();
    this.mockCount = mockCount;
    this.host = host;
  }

  public void constructEvents() {
    for (int i = 0; i < mockCount; i++) {
      EventBuilder eventBuilder = getConcreteEventBuilder();
      director(eventBuilder);
      addEventsToAction(eventBuilder);
    }
  }

  public void director(EventBuilder eventBuilder) {
    eventBuilder.resetTimeStamp();
    eventBuilder.buildPropertyValues();
    eventBuilder.buildEventValues();
    eventBuilder.buildSpecificAttribute();

  }

  public List<Event> getEvents() {
    logger.info("events collection ids: {}",collect(events,"id"));
    return events;
  }

  public void threadSleepFor1s() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  @Override
  public void addEventsToAction(EventBuilder eventBuilder) {
    switch (eventBuilder.getEventType()) {
      case COMPOSITE:
        CompositeEvent compositeEvent = eventBuilder.getCompositeEvent();
        events.add(compositeEvent.getParent());
        logger.debug("******add data. event parent id = {}, event description = {}",compositeEvent.getParent().getId(),compositeEvent.getParent());
        events.add(compositeEvent.getChild());
        logger.debug("******add data. event child id = {}, event description = {}",compositeEvent.getChild().getId(),compositeEvent.getChild());
        break;
      case LEAF:
        events.add(eventBuilder.getEvent());
        logger.debug("******add data. event is leaf. event id = {}. event description = {}", eventBuilder.getEvent().getId(), eventBuilder.getEvent());
        break;
      default:
        throw new UnsupportedOperationException("internal error");
    }

  }

  public EventBuilder getConcreteEventBuilder() {
    //Do not use the method.
    throw new UnsupportedOperationException(
        "Do not use this method.");
  }
}

