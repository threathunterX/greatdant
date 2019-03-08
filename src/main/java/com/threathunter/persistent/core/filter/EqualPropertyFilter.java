package com.threathunter.persistent.core.filter;

/**
 * Created by daisy on 17/7/14.
 */
public class EqualPropertyFilter implements PropertyFilter {

    private String param;

    public EqualPropertyFilter(final String parameter) {
        this.param = parameter;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return value.equals(param);
    }
}
