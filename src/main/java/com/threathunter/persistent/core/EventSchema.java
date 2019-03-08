package com.threathunter.persistent.core;

import com.threathunter.model.Property;

import java.util.List;
import java.util.Map;

/**
 * Created by daisy on 16-4-7.
 */
public interface EventSchema {

    List<String> getBodyProperties();

    List<Property> getProperties();

    String getLogType();
}
