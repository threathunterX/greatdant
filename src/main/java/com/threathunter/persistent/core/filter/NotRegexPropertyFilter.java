package com.threathunter.persistent.core.filter;

import java.util.regex.Pattern;

/**
 * 
 */
public class NotRegexPropertyFilter implements PropertyFilter {
    private String param;

    public NotRegexPropertyFilter(final String parameter) {
        this.param = parameter;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return !Pattern.matches(param, value);
    }
}
