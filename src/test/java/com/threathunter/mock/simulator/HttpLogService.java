package com.threathunter.mock.simulator;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.RemoteException;
import com.threathunter.babel.rpc.impl.ServiceClientImpl;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by yy on 17-8-8.
 */
public class HttpLogService implements EventsActionVisitor {

  private final ServiceClientImpl client;
  private final ServiceMeta meta;
  Logger logger = LoggerFactory.getLogger(HttpLogService.class);


  public HttpLogService() {
    CommonDynamicConfig.getInstance().addConfigFile("server.properties");
    CommonDynamicConfig.getInstance().addConfigFile("httplog_redis.service");
    this.meta = ServiceMetaUtil.getMetaFromResourceFile("httplog_redis.service");
    this.client = new ServiceClientImpl(this.meta);
  }


  private void execAction(EventsAction action) {
    logger.info(">>>>>>>>>>send data to server start, action instance: {}",action.hashCode());
    this.client.start();
    action.constructEvents();
    List<Event> events = action.getEvents();
    try {
      client.notify(events, meta.getName());
    } catch (RemoteException e) {
      logger.debug("service meta name: {}, meta info: {}",meta.getName(),meta);
     logger.error("error when call babel service from client",e);
    }
    this.client.stop();
    logger.info(">>>>>>>>>>send data to server end, action instance: {}", action.hashCode());
  }

  public void visit(AccountLoginEventsAction action) {
    execAction(action);
  }

  public void visit(HttpDynamicEventsAction action) {
    execAction(action);
  }

  public void visit(OrderSubmitEventsAction action) {
    execAction(action);
  }

  public void visit(CommonEventsAction action) {
    execAction(action);
  }

  public void visit(BullBuyerEventsAction action) {
    execAction(action);
  }

  public void visit(CheckInRobotEventsAction action) {
    execAction(action);
  }

  public void visit(RegistrationRobotEventsAction action) {
    execAction(action);
  }

  public void visit(EventsAction action) {
    action.apply(this);
  }


}
