package com.threathunter.mock.simulator;

import com.threathunter.model.Event;

import java.util.Map;

/**
 * 
 */
public interface EventBuilder {

  void buildPropertyValues();

  void buildEventValues();

  void buildSpecificAttribute();

  Event getEvent();

  CompositeEvent getCompositeEvent();

  void resetTimeStamp();

  Map<String, Object> getProp();

  EventBuilderType getEventType();
}
