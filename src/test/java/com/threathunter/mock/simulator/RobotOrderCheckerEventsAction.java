package com.threathunter.mock.simulator;

import java.util.HashMap;
import java.util.Map;

import static com.threathunter.mock.util.PropertyUtil.*;

/**
 * Created by yy on 17-8-11.
 */
public class RobotOrderCheckerEventsAction extends CommonEventsAction {

  String ip;
  String sid;
  String did;
  Map<String, Object> customProps;

  public RobotOrderCheckerEventsAction(String host, int mockCount) {
    super(host, mockCount);
    customProps = new HashMap<>();
    init();
  }

  private void init() {
    ip = getRandomIP();
    sid = getRandomStr(15);
    did = getRandomDevice();
    customProps.put("sid", sid);
    customProps.put("c_ip", ip);
    customProps.put("xforward", ip);
    customProps.put("did", did);


  }

  @Override
  public void constructEvents() {
    for (int i = 0; i < mockCount; i++) {
      threadSleepFor1s();
      String user = getRandomStr(10) + "@mail.com";
      customProps.put("use_name", user);
      EventBuilder loginEventBuilder=new AccountLoginEventBuilder(host,ip);
      super.director(loginEventBuilder);
      loginEventBuilder.getProp().putAll(customProps);
      addEventsToAction(loginEventBuilder);
      Map<String,Object> httpEventProps=new HashMap<>();
      httpEventProps.putAll(customProps);
      httpEventProps.put("uri_stem","/orderdetail");
      httpEventProps.put("s_body","{'code':200,'goodList':[{'goodName':'abc'}");
      httpEventProps.put("c_body","{'goodName':'bcd'}]}");
      threadSleepFor1s();
      EventBuilder httpDynamicBuilder=new HttpDynamicEventBuilder(host,ip);
      super.director(httpDynamicBuilder);
      httpDynamicBuilder.getProp().putAll(httpEventProps);
      addEventsToAction(httpDynamicBuilder);
      if (i % 5 == 0) {//每5轮换一下IP和SESSION，设备不换
        ip = getRandomIP();
        sid = getRandomStr(15);
        customProps.put("sid", sid);
        customProps.put("c_ip", ip);
      }
    }
  }

  @Override
  public void apply(EventsActionVisitor vistor) {
    vistor.visit(this);
  }


}
