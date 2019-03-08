package com.threathunter.persistent.core;

/**
 * Created by yy on 17-8-23.
 */
public enum EventArgumentErrorType {
  NO_ERROR(true, ""), COLS(false, "cols empty"), TERMS(false, "terms empty"), NAME(false,
      "event name empty"), TYPE(false, "action type empty"), ID(false, "id empty or null"), PAGE(
      false, "page empty or null");
  private final boolean valid;
  private String errorMsg;

  private EventArgumentErrorType(boolean valid, String errorMsg) {
    this.valid = valid;
    this.errorMsg = errorMsg;
  }

  public boolean isValid() {
    return valid;
  }


  public String getErrorMsg() {
    return errorMsg;
  }
}
