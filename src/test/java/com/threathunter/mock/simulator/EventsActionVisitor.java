package com.threathunter.mock.simulator;

/**
 * Created by yy on 17-8-7.
 */
public interface EventsActionVisitor {

  void visit(HttpDynamicEventsAction action);

  void visit(AccountLoginEventsAction action);

  void visit(OrderSubmitEventsAction action);

  void visit(CommonEventsAction action);

  void visit(BullBuyerEventsAction action);

  void visit(CheckInRobotEventsAction action);

  void visit(RegistrationRobotEventsAction action);

  void visit(EventsAction action);
}
