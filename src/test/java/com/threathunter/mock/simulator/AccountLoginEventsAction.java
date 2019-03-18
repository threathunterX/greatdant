package com.threathunter.mock.simulator;


import com.threathunter.mock.util.PropertyUtil;

/**
 * 
 */
public class AccountLoginEventsAction extends CommonEventsAction {

  private String ip;

  public AccountLoginEventsAction(String host, int mockCount) {
    super(host, mockCount);
    ip = PropertyUtil.getRandomIP();
  }

  public AccountLoginEventsAction(String host, int mockCount,String ip) {
    super(host, mockCount);
    this.ip = ip;
  }
  @Override
  public void apply(EventsActionVisitor vistor) {
    vistor.visit(this);
  }

  @Override
  public void addEventsToAction(EventBuilder eventBuilder) {
    CompositeEvent compositeEvent=eventBuilder.getCompositeEvent();
    events.add(compositeEvent.getParent());
    logger.debug("******add composite data. event parent id = {}, event description = {}",compositeEvent.getParent().getId(),compositeEvent.getParent());
    events.add(compositeEvent.getChild());
    logger.debug("******add composite data. event child id = {}, event description = {}",compositeEvent.getChild().getId(),compositeEvent.getChild());
  }

  @Override
  public EventBuilder getConcreteEventBuilder() {
    return new AccountLoginEventBuilder(host, ip);
  }
}
