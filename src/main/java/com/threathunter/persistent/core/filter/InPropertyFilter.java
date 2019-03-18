package com.threathunter.persistent.core.filter;

import java.util.Set;

/**
 * 
 */
public class InPropertyFilter implements PropertyFilter {

    private Set<String> params;

    public InPropertyFilter(final Set<String> parameters) {
        this.params = parameters;
    }

    @Override
    public boolean match(final String value) {
        if (value == null) {
            return false;
        }
        return params.contains(value);
    }
}
