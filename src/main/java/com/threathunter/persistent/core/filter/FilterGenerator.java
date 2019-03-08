package com.threathunter.persistent.core.filter;

import java.util.*;

/**
 * Created by daisy on 17/7/14.
 */
public class FilterGenerator {

    public static Map<String, PropertyFilter> generateFilters(final List<Map<String, Object>> terms) {
        Map<String, PropertyFilter> filterMap = new HashMap<>();

        terms.forEach(term -> {
            String left = (String) term.get("left");
            if (!left.equals("name")) {
                try {
                    PropertyFilter filter = createFilter((String) term.get("op"), term.get("right"));
                    if (filter == null) {
                        throw new RuntimeException("filter op is not support");
                    }
                    filterMap.put(left, filter);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("create filter error, op: %s, value: %s", term.get("op"),
                            term.get("right")), e);
                }
            }
        });

        return filterMap;
    }


    private static PropertyFilter createFilter(final String operation, final Object value) {
        switch (operation) {
            case "==":
                return new EqualPropertyFilter((String) value);
            case "!=":
                return new NotEqualPropertyFilter((String) value);
            case "contain":
                return new ContainsPropertyFilter((String) value);
            case "!contain":
                return new NotContainsPropertyFilter((String) value);
            case "in":
                return new InPropertyFilter(new HashSet<>((Collection) value));
            case "!in":
                return new NotInPropertyFilter(new HashSet<>((Collection) value));
            case "regex":
                return new RegexPropertyFilter((String) value);
            case "!regex":
                return new NotRegexPropertyFilter((String) value);
            default:
                return null;
        }
    }
}
