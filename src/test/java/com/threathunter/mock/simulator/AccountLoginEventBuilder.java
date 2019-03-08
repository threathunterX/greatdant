package com.threathunter.mock.simulator;

import com.threathunter.common.ObjectId;
import com.threathunter.mock.util.PropertyUtil;
import com.threathunter.model.Event;

/**
 * Created by yy on 17-8-8.
 */
public class AccountLoginEventBuilder extends CommonEventBuilder {

  private String httpeventid;
  private CompositeEvent compositeEvent;

  public AccountLoginEventBuilder(String host, String ip) {
    super(host, ip);
    httpeventid = new ObjectId().toHexString();
    compositeEvent = new CompositeEvent();
  }

  public AccountLoginEventBuilder(String host) {
    super(host);
  }

  public void buildPropertyValues() {
    String user = PropertyUtil.getRandomUser() + "@mail.com";

    prop.put("referer", host);
    prop.put("c_port", 4430);
    prop.put("request_type", "click");
    prop.put("s_ip", "10.1.0.10");
    prop.put("s_body", PropertyUtil.getRandomStr(20));
    prop.put("useragent", "java-0.8.11");
    prop.put("platform", "PC");
    prop.put("sid", PropertyUtil.getRandomStr(10));
    prop.put("uid", user);
    prop.put("request_time", ts);
    prop.put("host", host);
    prop.put("s_port", 80);
    prop.put("method", "POST");
    prop.put("cookie", "");
    prop.put("c_body", PropertyUtil.getRandomStr(20));
    prop.put("s_type", "application/json");
    prop.put("c_ip", ip);
    prop.put("uri_stem", "/user/login");
    prop.put("uri_query", "q=" + PropertyUtil.getRandomStr(5));
    prop.put("c_type", "application/json");
    prop.put("s_bytes", 23);
    prop.put("page", host + "/user/login");
    prop.put("xforward", ip);
    prop.put("did", PropertyUtil.getRandomDevice());
    prop.put("referer_hit", "F");
    prop.put("status", 200);
    prop.put("password", PropertyUtil.getRandomStr(10));
    prop.put("result", "F");
    prop.put("captcha", PropertyUtil.getRandomStr(4));
    prop.put("user_name", user);
  }

  public void buildEventValues() {
    Event e = new Event();
    e.setApp("nebula");
    e.setId(httpeventid);
    e.setKey(ip);
    e.setName("HTTP_DYNAMIC");
    e.setTimestamp(ts);
    e.setValue(1.0);
    e.setPropertyValues(prop);
    getCompositeEvent().setParent(e);
  }

  public void buildSpecificAttribute() {
    Event eC = new Event();
    eC.setPid(httpeventid);
    eC.setApp("nebula");
    eC.setId(new ObjectId().toHexString());
    eC.setKey(ip);
    eC.setName("ACCOUNT_LOGIN");
    eC.setTimestamp(ts);
    eC.setValue(1.0);
    eC.setPropertyValues(prop);
    getCompositeEvent().setChild(eC);
  }


  @Override
  public CompositeEvent getCompositeEvent() {
    return compositeEvent;
  }

  @Override
  public EventBuilderType getEventType() {
    return EventBuilderType.COMPOSITE;
  }
}
