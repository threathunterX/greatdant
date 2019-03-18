package com.threathunter.mock.simulator;

import java.util.HashMap;
import java.util.Map;

import static com.threathunter.mock.util.PropertyUtil.*;


/**
 * 
 */
public class BullBuyerEventsAction extends CommonEventsAction {

  Map<String, Object> customProps;
  private String ip;
  private String sid;
  private String did;
  private String mobile;

  public BullBuyerEventsAction(String host, int mockCount) {
    super(host, mockCount);
    ip = getRandomIP();
    sid = getRandomStr(15);
    did = getRandomDevice();
    mobile = getRandomMobile();
    init();
  }

  private void init() {
    customProps = new HashMap<>();
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
    for (int i = 0; i < this.mockCount; i++) {
      String user = getRandomUser();
      EventBuilder builder = new AccountLoginEventBuilder(host, ip);
      director(builder);
      customProps.put("uid", user);
      customProps.put("user_name", user);
      builder.getProp().putAll(customProps);
      addEventsToAction(builder);
      this.threadSleepFor1s();
      String order_id = "order" + getRandomStr(15);
      String transaction_id = "transaction" + getRandomStr(10);
      Map<String, Object> orderProps = new HashMap<>();
      orderProps.putAll(customProps);
      orderProps.put("receiver_mobile", mobile);
      orderProps.put("receiver_address_country", "中国");
      orderProps.put("receiver_address_province", "上海");
      orderProps.put("receiver_address_city", "上海");
      orderProps.put("receiver_address_detail", "人民广场花园旁转盘1号出口");
      orderProps.put("receiver_realname", "王大爷" + getRandomStr(2));
      orderProps.put("order_id", order_id);
      orderProps.put("transaction_id", transaction_id);
      builder = new OrderSubmitEventBuilder(host, ip);
      director(builder);
      builder.getProp().putAll(orderProps);
      addEventsToAction(builder);
      this.threadSleepFor1s();
      Map<String, Object> transProps = new HashMap<>();
      transProps.putAll(customProps);
      transProps.put("transaction_id", transaction_id);
      transProps.put("escrow_type", "微信");
      transProps.put("escrow_account", "wx_" + getRandomStr(10));
      transProps.put("pay_amount", 100);
      builder = new TransactionEventBuilder(host, sid);
      director(builder);
      builder.getProp().putAll(transProps);
      addEventsToAction(builder);
      if (i % 5 == 0) {
        ip = getRandomIP();//每5次订单切换IP和SESSION
        sid = getRandomStr(15);
      }
    }
  }
}
