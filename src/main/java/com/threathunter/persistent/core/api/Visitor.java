package com.threathunter.persistent.core.api;

import com.threathunter.model.Event;

/**
 * Created by yy on 17-11-20.
 */
public interface Visitor {
    void visit(Event event);
}
