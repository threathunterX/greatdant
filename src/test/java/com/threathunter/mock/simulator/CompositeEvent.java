package com.threathunter.mock.simulator;

import com.threathunter.model.Event;

/**
 * 
 */
public class CompositeEvent {

  private Event parent;
  private Event child;

  public CompositeEvent() {
  }

  public Event getParent() {
    return parent;
  }

  public void setParent(Event parent) {
    this.parent = parent;
  }

  public Event getChild() {
    return child;
  }

  public void setChild(Event child) {
    this.child = child;
  }
}
