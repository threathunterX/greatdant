package com.threathunter.mock.simulator;

import com.threathunter.model.Event;

import java.util.List;

/**
 * Created by yy on 17-8-7.
 */
public interface EventsAction {

  public static final String SPIDER_FINISHED = "SPIDER ACTION FINISHED";
  public static final String ACCOUNT_BRUTEFORCE_FINISHED = "ACCOUNT BRUTEFORCE FINISHED";
  public static final String ORDER_SUBMIT_FINISHED = "ORDER SUBMIT FINISHED";

  void constructEvents();

  void apply(EventsActionVisitor vistor);

  List<Event> getEvents();

  void addEventsToAction(EventBuilder eventBuilder);
}
