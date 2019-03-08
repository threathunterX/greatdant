package com.threathunter.persistent.core;

import com.threathunter.persistent.core.api.LogsReadContext;

/**
 * Created by yy on 17-9-15.
 */
public interface LogsRead {

  void read(LogsReadContext context);
}
