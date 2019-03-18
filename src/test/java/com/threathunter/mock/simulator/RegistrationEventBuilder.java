package com.threathunter.mock.simulator;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;

import static com.threathunter.mock.util.PropertyUtil.*;


/**
 * 
 */
public class RegistrationEventBuilder extends CommonEventBuilder {

  private final String usere;
  private final String sid;
  private final String did;
  private final String user;
  private final String httpeventid;
  private final CompositeEvent compositeEvent;

  public RegistrationEventBuilder(String host, String ip) {
    super(host, ip);
    this.user = getRandomUser();
    usere = user + "@mail.com";
    sid = getRandomStr(10);
    did = getRandomDevice();
    httpeventid = new ObjectId().toHexString();
    compositeEvent = new CompositeEvent();
  }

  @Override
  public void buildPropertyValues() {
    prop.put("referer", host);
    prop.put("c_port", 4430);
    prop.put("request_type", "click");
    prop.put("s_ip", "10.1.0.10");
    prop.put("s_body", getRandomStr(20));
    prop.put("useragent", "java-0.8.11");
    prop.put("platform", "PC");
    prop.put("sid", sid);
    prop.put("uid", usere);
    prop.put("request_time", ts);
    prop.put("host", host);
    prop.put("s_port", 80);
    prop.put("method", "POST");
    prop.put("cookie", "");
    prop.put("c_body", getRandomStr(20));
    prop.put("s_type", "application/json");
    prop.put("c_ip", ip);
    prop.put("uri_stem", "/user/regist");
    prop.put("uri_query", "q=" + getRandomStr(5));
    prop.put("c_type", "application/json");
    prop.put("s_bytes", 23);
    prop.put("page", host + "/user/regist");
    prop.put("xforward", ip);
    prop.put("did", did);
    prop.put("referer_hit", "F");
    prop.put("status", 200);
    prop.put("password", getRandomStr(10));
    prop.put("result", "T");
    prop.put("captcha", getRandomStr(4));
    prop.put("user_name", usere);
    prop.put("register_verfication_token", "");
    prop.put("register_verfication_token_type", "");
    prop.put("register_realname", "");
    prop.put("register_channel", "");
    prop.put("invite_code", "vip");
  }

  @Override
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

  @Override
  public void buildSpecificAttribute() {
    Event eC = new Event();
    eC.setPid(httpeventid);
    eC.setApp("nebula");
    eC.setId(new ObjectId().toHexString());
    eC.setKey(ip);
    eC.setName("ACCOUNT_REGISTRATION");
    eC.setTimestamp(ts);
    eC.setValue(1.0);
    eC.setPropertyValues(prop);
    getCompositeEvent().setChild(eC);
  }

  @Override
  public CompositeEvent getCompositeEvent() {
    return this.compositeEvent;
  }

  @Override
  public EventBuilderType getEventType() {
    return EventBuilderType.COMPOSITE;
  }
}
