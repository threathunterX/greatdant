package com.threathunter.persistent.core.filter;

/**
 * Created by daisy on 17/7/14.
 */
public class NotContainsPropertyFilter implements PropertyFilter {

    private String param;

    public NotContainsPropertyFilter(final String parameter) {
        this.param = parameter;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return !value.contains(param);
    }
}
