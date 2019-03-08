package com.threathunter.persistent.core;

/**
 * Created by yy on 17-8-22.
 */
public enum ProgressState {
  START("start"), PROGRESSING("process"),WAITING("wait"), CANCELLING("wait"), CANCELLED("wait"), END("success"), ERROR("failed");
  private String value;
  private ProgressState(String value){
    this.value=value;
  }
  public String getValue(){
    return value;
  }
}
