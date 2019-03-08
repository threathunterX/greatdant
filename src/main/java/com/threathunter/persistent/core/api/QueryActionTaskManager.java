package com.threathunter.persistent.core.api;

import com.threathunter.model.Event;
import com.threathunter.persistent.core.ProgressState;
import com.threathunter.persistent.core.QueryActionTask;
import com.threathunter.persistent.core.util.EventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yy on 17-8-22.
 */
public class QueryActionTaskManager {

  private static QueryActionTaskManager instance = new QueryActionTaskManager();
  private static Logger logger = LoggerFactory.getLogger(QueryActionTaskManager.class);
  private Map<String, QueryActionTask> tasks = new HashMap<>();
  private static ExecutorService pool= Executors.newFixedThreadPool(2);


  private QueryActionTaskManager() {
  }

  public static QueryActionTaskManager getInstance() {
    return instance;
  }


  public void register(QueryActionTask actionTask, String id) {
    tasks.put(id, actionTask);
    actionTask.setState(ProgressState.WAITING);
    pool.submit(actionTask);
  }


  public boolean executeActionTask(Event event, QueryActionType type) {
    String requestId = EventUtil.getRequestId(event);
    logger.info("execute task, requestId = {}, actionType = {}", requestId, type.toString());
    if (type.equals(QueryActionType.CREATE)) {
      new QueryActionTask(requestId, event, this);
      ProgressState state = getState(requestId);
      if (state == ProgressState.START || state == ProgressState.PROGRESSING || state==ProgressState.WAITING|| state==ProgressState.END) {
        return true;
      }
    } else if (type.equals(QueryActionType.DELETE)) {
      if(!tasks.containsKey(requestId))
        return true;
      tasks.get(requestId).cancel();
      ProgressState state = getState(requestId);
      if (state == ProgressState.CANCELLING || state == ProgressState.END) {
        return true;
      }
    } else if (type.equals(QueryActionType.FETCH)) {
      ProgressState state = getState(requestId);
      if (state == ProgressState.END) {
        return true;
      }
    }
    return false;
  }

  private ProgressState getState(String id) {
    QueryActionTask task = tasks.get(id);
    if (task == null) {
      return null;
    }
    return task.getState();
  }

  public Map<String, QueryActionTask> getTasks() {
    return tasks;
  }
}
