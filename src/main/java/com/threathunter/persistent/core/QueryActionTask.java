package com.threathunter.persistent.core;

import com.threathunter.model.Event;
import com.threathunter.persistent.core.api.LogsReadContext;
import com.threathunter.persistent.core.api.QueryActionTaskManager;
import com.threathunter.persistent.core.util.EventUtil;
import com.threathunter.persistent.core.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by yy on 17-8-22.
 */
public class QueryActionTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(QueryActionTask.class);
  private volatile ProgressState state;
  private Event event;
  private String id;
  private volatile boolean running;
  private BlockingQueue<Exception> queue=new LinkedBlockingDeque<>();
  private LogsReadContext context;

  public QueryActionTask(String id, Event event, QueryActionTaskManager manager) {
    this.id = id;
    this.event = event;
    state = ProgressState.START;
    running = true;
    context= EventUtil.asLogsQueyContext(event);
    manager.register(this, id);
  }

  @Override
  public void run() {
    try {
      while (running && !done()) {
        dowork();
        state = ProgressState.END;
        if (state == ProgressState.END) {
          IOUtil.writeTotal(id, this.getTotalItems());
        }
        Thread.sleep(30);
      }
      if (!running) {
        state = ProgressState.CANCELLED;
      }
    } catch (InterruptedException e) {
      logger.info("query action: id = {} is cancelled.");
      state = ProgressState.CANCELLED;
    }


  }

  private void dowork() throws InterruptedException {
    state = ProgressState.PROGRESSING;
    try{
      context.startQuery();
      }
     catch (Exception e) {
      queue.add(e);
      logger.error("query and write data error", e);
      state = ProgressState.ERROR;
    } finally {
    context.endQuery();
    }

  }


  public void cancel() {
    if (state == ProgressState.END) {
      return;
    }
    state = ProgressState.CANCELLING;
    running = false;
    try {
      logger.info("current thread sleep for 1 second to enable thread cancel");
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
  }


  public boolean done() {
    if (state.equals(ProgressState.END)) {
      logger.info("task action is done. request id = {}", id);
      return true;
    }
    return false;
  }


  public ProgressState getState() {
    return state;
  }

  public void setState(ProgressState state){
    this.state=state;
  }

  public Double getProgress() {
    return  context.getProgress();
  }

  public int getTotalItems() {
    return context.getTotalItem();
  }

  public String getErrorMessage() {
    StringBuilder builder=new StringBuilder();
    queue.forEach((item)->builder.append(item.getMessage()));
    return builder.toString();
  }
}
