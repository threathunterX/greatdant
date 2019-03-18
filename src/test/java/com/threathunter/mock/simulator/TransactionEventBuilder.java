package com.threathunter.mock.simulator;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;

import java.util.HashMap;
import java.util.Map;

import static com.threathunter.mock.util.PropertyUtil.*;

/**
 * 
 */
public class TransactionEventBuilder extends CommonEventBuilder {

  private final String user;
  private final String sid;
  private final String did;
  private final String httpeventid2;
  private final CompositeEvent compositeEvent;

  public TransactionEventBuilder(String host, String ip) {
    super(host, ip);
    user = getRandomUser();
    sid = getRandomStr(10);
    did = getRandomDevice();
    this.compositeEvent = new CompositeEvent();
    httpeventid2 = new ObjectId().toHexString();
  }

  @Override
  public void buildPropertyValues() {
    Map<String, Object> prop2 = new HashMap<String, Object>();
    prop.put("referer", host);
    prop.put("c_port", 4430);
    prop.put("request_type", "click");
    prop.put("s_ip", "10.1.0.10");
    prop.put("s_body", getRandomStr(20));
    prop.put("useragent", "java-0.8.11");
    prop.put("platform", "PC");
    prop.put("sid", sid);
    prop.put("uid", user);
    prop.put("request_time", ts);
    prop.put("host", host);
    prop.put("s_port", 80);
    prop.put("method", "POST");
    prop.put("cookie", "");
    prop.put("c_body", getRandomStr(20));
    prop.put("s_type", "application/json");
    prop.put("c_ip", ip);
    prop.put("uri_stem", "/order/pay");
    prop.put("uri_query", "q=" + getRandomStr(5));
    prop.put("c_type", "application/json");
    prop.put("s_bytes", 23);
    prop.put("page", host + "/order/pay");
    prop.put("xforward", ip);
    prop.put("did", did);
    prop.put("referer_hit", "F");
    prop.put("status", 200);
    prop.put("user_name", user);
    /*
    can be added by prop by getProp();
    //TODO
    prop.put("transaction_id",transcation_id);
    prop.put("escrow_type",escrow_type);
    prop.put("escrow_account",escrow_account);
    prop.put("pay_amount",pay_amount);
    */
    prop.put("result", "T");


  }

  @Override
  public void buildEventValues() {
    Event e1 = new Event();
    e1.setApp("nebula");
    e1.setId(httpeventid2);
    e1.setKey(ip);
    e1.setName("HTTP_DYNAMIC");
    e1.setTimestamp(ts);
    e1.setValue(1.0);
    e1.setPropertyValues(prop);
    this.getCompositeEvent().setParent(e1);

  }

  @Override
  public void buildSpecificAttribute() {
    Event eO = new Event();
    eO.setPid(httpeventid2);
    eO.setApp("nebula");
    eO.setId(new ObjectId().toHexString());
    eO.setKey(ip);
    eO.setName("TRANSACTION_ESCOW");
    eO.setTimestamp(ts);
    eO.setValue(1.0);
    eO.setPropertyValues(prop);
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
