package com.threathunter.persistent.core.api;

public enum QueryActionType {
  CREATE("create"), DELETE("delete"), FETCH("fetch");

  private String type;

  QueryActionType(String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return type;
  }

}