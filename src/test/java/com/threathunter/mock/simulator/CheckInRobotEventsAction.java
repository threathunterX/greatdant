package com.threathunter.mock.simulator;

import java.util.HashMap;
import java.util.Map;

import static com.threathunter.mock.util.PropertyUtil.*;


/**
 * 
 */
public class CheckInRobotEventsAction extends CommonEventsAction {


  Map<String, Object> customProps;
  private String ip;
  private String sid;
  private String did;

  public CheckInRobotEventsAction(String host, int mockCount) {
    super(host, mockCount);
    ip = getRandomIP();
    sid = getRandomStr(15);
    did = getRandomDevice();
    customProps = new HashMap<>();
    init();
  }

  private void init() {
    customProps.put("sid", sid);
    customProps.put("c_ip", ip);
    customProps.put("xforward", ip);
    customProps.put("did", did);
  }

  @Override
  public void apply(EventsActionVisitor vistor) {
    vistor.visit(this);

  }

  @Override
  public void constructEvents() {
    for (int i = 0; i < mockCount; i++) {
      String user = getRandomUser();
      customProps.put("uid", user);
      customProps.put("user_name", user);
      EventBuilder builder = new AccountLoginEventBuilder(host, ip);
      director(builder);
      builder.getProp().putAll(customProps);
      addEventsToAction(builder);
      threadSleepFor1s();
      HttpDynamicEventBuilder httpDynamicBuilder = new HttpDynamicEventBuilder(host, ip);
      Map<String, Object> httpEventProps = new HashMap<>();
      httpEventProps.putAll(customProps);
      httpEventProps.put("uri_stem", "/my_sign/");
      httpEventProps.put("s_body", "{'code':200,'msg':'sign ok'}");
      httpEventProps.put("c_body", null);
      httpEventProps.put("uri_query", user + "@mail.com");
      httpEventProps.put("uid", user + "@mail.com");
      super.director(httpDynamicBuilder);
      httpDynamicBuilder.getProp().putAll(httpEventProps);
      addEventsToAction(httpDynamicBuilder);
      if (i % 5 == 0) {//每5轮换一下IP和SESSION，设备不换
        ip = getRandomIP();
        sid = getRandomStr(15);
        customProps.put("sid", sid);
        customProps.put("c_ip", ip);
      }
      threadSleepFor1s();
    }

  }
}
