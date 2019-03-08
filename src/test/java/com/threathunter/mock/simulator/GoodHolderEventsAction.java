package com.threathunter.mock.simulator;

/**
 * Created by yy on 17-8-10.
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
