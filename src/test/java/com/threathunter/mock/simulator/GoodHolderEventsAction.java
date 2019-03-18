package com.threathunter.mock.simulator;

/**
 * 
 */
public class GoodHolderEventsAction extends CommonEventsAction {


  public GoodHolderEventsAction(String host, int mockCount) {
    super(host, mockCount);
  }

  @Override
  public void apply(EventsActionVisitor vistor) {

  }

  @Override
  public void addEventsToAction(EventBuilder eventBuilder) {

  }

  @Override
  public EventBuilder getConcreteEventBuilder() {
    return null;
  }
}
