package com.threathunter.persistent.core.filter;

/**
 * 
 */
public class ContainsPropertyFilter implements PropertyFilter {

    private String param;

    public ContainsPropertyFilter(final String parameter) {
        this.param = parameter;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return value.contains(param);
    }
}
