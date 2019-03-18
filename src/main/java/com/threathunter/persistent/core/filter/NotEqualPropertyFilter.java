package com.threathunter.persistent.core.filter;

/**
 * 
 */
public class NotEqualPropertyFilter implements PropertyFilter {

    private String param;

    public NotEqualPropertyFilter(final String parameter) {
        this.param = parameter;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return !value.equals(param);
    }
}
