package com.threathunter.persistent.core.filter;

import java.util.regex.Pattern;

/**
 * Created by daisy on 17/7/14.
 */
public class RegexPropertyFilter implements PropertyFilter {

    private String param;

    public RegexPropertyFilter(final String parameter) {
        this.param = parameter;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return Pattern.matches(this.param, value);
    }
}
