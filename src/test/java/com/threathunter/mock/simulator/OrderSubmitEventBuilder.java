package com.threathunter.mock.simulator;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;

import static com.threathunter.mock.util.PropertyUtil.getRandomMobile;
import static com.threathunter.mock.util.PropertyUtil.getRandomStr;

/**
 * Created by yy on 17-8-9.
 */
public class OrderSubmitEventBuilder extends CommonEventBuilder {

  String httpeventid;
  private long ts;
  private CompositeEvent compositeEvent;

  public OrderSubmitEventBuilder(String host, String ip) {
    super(host, ip);
    ts = System.currentTimeMillis();
    compositeEvent = new CompositeEvent();
    httpeventid = new ObjectId().toHexString();
  }

  @Override
  public void buildPropertyValues() {
    long ts2 = System.currentTimeMillis();
    prop.put("referer", host);
    prop.put("c_port", 4430);
    prop.put("request_type", "click");
    prop.put("s_ip", "10.1.0.10");
    prop.put("s_body", getRandomStr(20));
    prop.put("useragent", "java-0.8.11");
    prop.put("platform", "PC");
//    prop.put("sid",sid);
//    prop.put("uid",user);
    prop.put("request_time", ts2);
    prop.put("host", host);
    prop.put("s_port", 80);
    prop.put("method", "POST");
    prop.put("cookie", "");
    prop.put("c_body", getRandomStr(20));
    prop.put("s_type", "application/json");
    prop.put("c_ip", ip);
    prop.put("uri_stem", "/user/createorder");
    prop.put("uri_query", "q=" + getRandomStr(5));
    prop.put("c_type", "application/json");
    prop.put("s_bytes", 23);
    prop.put("page", host + "/user/createorder");
    prop.put("xforward", ip);
//    prop.put("did",did);
    prop.put("referer_hit", "F");
    prop.put("status", 200);
    prop.put("order_id", getRandomStr(10));
//    prop.put("user_name", user);
    prop.put("product_id", getRandomStr(10));
    prop.put("merchant", getRandomStr(10));
    prop.put("order_money_amount", (double) (Math.random() * 50 + 200));
    prop.put("order_coupon_amount", 0);
    prop.put("order_point_amount", 0);
    prop.put("transaction_id", getRandomStr(10));
    prop.put("receiver_mobile", getRandomMobile());
    prop.put("receiver_address_country", "中国");
    prop.put("receiver_address_province", "钓鱼岛");
    prop.put("receiver_address_city", "钓鱼岛");
    prop.put("receiver_address_detail", "钓鱼岛人民大道1号101室");
    prop.put("receiver_realname", "王建国");
    prop.put("result", "T");

  }

  @Override
  public void buildEventValues() {

    Event event = new Event();
    event.setApp("nebula");
    event.setId(httpeventid);
    event.setKey(ip);
    event.setName("HTTP_DYNAMIC");
    event.setTimestamp(ts);
    event.setValue(1.0);
    event.setPropertyValues(prop);
    getCompositeEvent().setParent(event);
  }

  @Override
  public void buildSpecificAttribute() {
    Event child = new Event();
    child.setPid(httpeventid);
    child.setApp("nebula");
    child.setId(new ObjectId().toHexString());
    child.setKey(ip);
    child.setName("ORDER_SUBMIT");
    child.setTimestamp(ts);
    child.setValue(1.0);
    child.setPropertyValues(prop);
    getCompositeEvent().setChild(child);
  }

  public CompositeEvent getCompositeEvent() {
    return compositeEvent;
  }

  @Override
  public EventBuilderType getEventType() {
    return EventBuilderType.COMPOSITE;
  }
}
