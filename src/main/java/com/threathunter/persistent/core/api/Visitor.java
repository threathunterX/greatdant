package com.threathunter.persistent.core.api;

import com.threathunter.model.Event;

/**
 * 
 */
public interface Visitor {
    void visit(Event event);
}
