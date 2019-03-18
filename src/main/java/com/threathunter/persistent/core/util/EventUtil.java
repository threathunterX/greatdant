package com.threathunter.persistent.core.util;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;
import com.threathunter.persistent.core.*;
import com.threathunter.persistent.core.api.LogsReadContext;
import com.threathunter.persistent.core.api.LogsReadContextBuilder;
import com.threathunter.persistent.core.api.QueryActionType;
import com.threathunter.persistent.core.filter.FilterGenerator;
import com.threathunter.persistent.core.filter.PropertyFilter;
import com.threathunter.persistent.core.io.QueryCSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class EventUtil {

    public static String DEFAULT_OBJECT_ID = "000000000000000000000000";
    private static Logger LOGGER = LoggerFactory.getLogger(EventUtil.class);
    private static DecimalFormat decimalFormatter = new DecimalFormat("#.##");

    public static String getEventName(Event event) {
        return (String) getPropertyValueByName(event, "name");
    }

    public static EventArgumentErrorType checkEventArgument(Event event) {
        EventArgumentErrorType type = EventArgumentErrorType.NO_ERROR;
        Map<String, Object> propertyValues = event.getPropertyValues();
        String requestId = getRequestId(event);
        String actionType = (String) propertyValues.get("action_type");
        if (requestId == null || requestId.isEmpty()) {
            LOGGER.error("request id is null or empty, evnet requestId {}." + requestId);
            type = EventArgumentErrorType.ID;
            return type;
        }
        if (actionType == null || actionType.isEmpty()) {
            LOGGER.error("action type is null or empty, evnet requestId {}." + requestId);
            type = EventArgumentErrorType.TYPE;
            return type;
        }
        QueryActionType enumActionType = getActionType(event);
        switch (enumActionType) {
            case CREATE:
                return checkCreateEventArgument(event);
            case DELETE:
                return checkDeleteEventArgument(event);
            case FETCH:
                return checkFetchEventArgument(event);
        }
        return type;
    }

    private static EventArgumentErrorType checkFetchEventArgument(Event event) {
        EventArgumentErrorType type = EventArgumentErrorType.NO_ERROR;
        Map<String, Object> propertyValues = event.getPropertyValues();
        String requestId = getRequestId(event);
        String page = String.valueOf(propertyValues.get("page"));
        if (page == null || page.isEmpty()) {
            LOGGER.error("page empty or null, event requestId {}." + requestId);
            type = EventArgumentErrorType.PAGE;
        }
        return type;
    }


    private static EventArgumentErrorType checkDeleteEventArgument(Event event) {
        return EventArgumentErrorType.NO_ERROR;
    }

    public static EventArgumentErrorType checkCreateEventArgument(Event event) {
        EventArgumentErrorType type = EventArgumentErrorType.NO_ERROR;
        Map<String, Object> propertyValues = event.getPropertyValues();
        String requestId = getRequestId(event);
        List<String> showCols = (List<String>) propertyValues.get("show_cols");
        List<Map<String, Object>> terms = (List<Map<String, Object>>) propertyValues.get("terms");
        String eventName = EventUtil.getEventName(event);
        if (showCols == null || showCols.size() <= 0) {
            LOGGER.error("empty show cols, event requestId {}." + requestId);
            type = EventArgumentErrorType.COLS;
        }
        if (terms == null || terms.size() <= 0) {
            LOGGER.error("terms empty or null, event requestId {}." + requestId);
            type = EventArgumentErrorType.TERMS;
        }
        if (eventName == null || eventName.isEmpty()) {
            LOGGER.error("empty event name empty or null, evnet requestId {}." + requestId);
            type = EventArgumentErrorType.NAME;
        }

        return type;
    }

    public static String getRequestId(Event event) {
        Map<String, Object> propertyValues = event.getPropertyValues();
        String requestId = ((Long) propertyValues.get("id")).toString();
        return requestId;
    }

    public static Object getPropertyValueByName(Event event, String name) {
        Object ret = null;
        Map<String, Object> propertyValues = event.getPropertyValues();
        ret = propertyValues.get(name);
        return ret;
    }

    public static QueryActionType getActionType(Event event) {
        String action = (String) getPropertyValueByName(event, "action_type");
        if ("create".equalsIgnoreCase(action)) {
            return QueryActionType.CREATE;
        }
        if ("delete".equalsIgnoreCase(action)) {
            return QueryActionType.DELETE;
        }
        if ("fetch".equalsIgnoreCase(action)) {
            return QueryActionType.FETCH;
        }
        return null;
    }

    public static Event createResponseEvent(String requestId, EventArgumentErrorType type) {
        if (type.isValid()) {
            return null;
        }
        Map<String, Object> properties = new HashMap<>();
        properties.put("id", requestId);
        properties.put("success", false);
        properties.put("errmsg", type.getErrorMsg());
        properties.put("errtype", "invalid_params");

        Event event = new Event("__all__", "", "", System.currentTimeMillis(), 1.0, properties);
        event.setId(getRandomEventId());
        event.setPid(EventUtil.DEFAULT_OBJECT_ID);

        return event;
    }

    public static Event createResponseEvent(String requestId, boolean result, QueryActionType actionType) {
        Event event = null;
        switch (actionType) {
            case CREATE:
                event = newCreateResponseEvent(requestId, result);
                break;
            case DELETE:
                event = newDeleteResponseEvent(requestId, result);
                break;
            case FETCH:
                //need to load data, need page params
                //should call in standalone
//        event = newFetchResponseEvent(requestId, result);
                break;
        }
        return event;
    }

    private static Event newFetchResponseEvent(String requestId, boolean success) {

        return null;
    }

    private static Event newDeleteResponseEvent(String requestId, boolean success) {
        Map<String, Object> properties = new HashMap<>();
        if (success) {
            properties.put("id", requestId);
            properties.put("success", true);
        } else {

            properties.put("success", false);
            properties.put("errmsg", "delete persistence query error");
        }
        Event event = createCommonEvent(properties);

        return event;
    }

    private static Event newCreateResponseEvent(String requestId, boolean success) {
        Map<String, Object> properties = new HashMap<>();
        if (success) {
            properties.put("id", requestId);
            properties.put("success", true);
        } else {
            properties.put("id", requestId);
            properties.put("success", false);
            properties.put("errmsg", "create persistence query error");
            properties.put("errtype", "internal");
        }
        Event event = createCommonEvent(properties);

        return event;

    }

    public static Event createCommonEvent(
            Map<String, Object> properties) {
        Event event = new Event("__all__", "persistentquery_crud", "", System.currentTimeMillis(), 1.0,
                properties);
        event.setId(getRandomEventId());
        event.setPid(EventUtil.DEFAULT_OBJECT_ID);
        return event;
    }

    public static Event createNotifyCommonEvent(
            Map<String, Object> properties) {
        Event event = new Event("__all__", "persistentquery_progress", "", System.currentTimeMillis(),
                1.0,
                properties);
        event.setId(getRandomEventId());
        event.setPid(EventUtil.DEFAULT_OBJECT_ID);
        return event;
    }

    public static Event createProgressNotifyEvent(Map<String, QueryActionTask> tasks) {
        Map<String, Object> properties = new HashMap<>();
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (Map.Entry<String, QueryActionTask> entry : tasks.entrySet()) {
            Map<String, Object> itemMap = new HashMap<>();
            String requestId = entry.getKey();
            QueryActionTask task = entry.getValue();
            ProgressState state = task.getState();
//      if(Long.valueOf(requestId))
            itemMap.put("id", Long.valueOf(requestId));
            itemMap.put("status", state.getValue());
            if (state == ProgressState.PROGRESSING) {
                itemMap.put("progress", decimalFormatter.format(task.getProgress()));
                itemMap.put("error", null);
            } else if (state == ProgressState.ERROR) {
                itemMap.put("progress", null);
                itemMap.put("error", task.getErrorMessage());
            } else if (state == ProgressState.END) {
                String fileName = IOUtil.generateFileName(requestId);
                itemMap.put("download_path", fileName);
                Long filesize = queryFileSize(fileName);
                itemMap.put("filesize", filesize);
                itemMap.put("total", task.getTotalItems());
            } else if (state == ProgressState.WAITING) {
                itemMap.put("progress", null);
                itemMap.put("error", null);
            } else {
                itemMap.put("progress", null);
                itemMap.put("error", null);
            }
            dataList.add(itemMap);
        }
        properties.put("success", true);
        properties.put("data", dataList);
        return createNotifyCommonEvent(properties);
    }

    private static Long queryFileSize(String name) {
        String path = IOUtil.getPath(name);
        File file = new File(path);
        if (file != null && file.exists()) {
            return file.length();
        }
        return 0L;
    }

    public static String getRandomEventId() {
        return ObjectId.get().toHexString();
    }

    public static Event createFetchResponseEvent(String requestId, Event event) {
        Map<String, Object> properties = new HashMap<>();
        String page = String.valueOf(getPropertyValueByName(event, "page"));
        String pageSizeString = String.valueOf(getPropertyValueByName(event, "page_count"));
        properties.put("id", requestId);
        properties.put("page", Integer.valueOf(page));
        properties.put("page_count", Integer.valueOf(pageSizeString));

        String fileName = IOUtil.generateFileName(requestId);
        try {
            List<Map<String, Object>> result = IOUtil
                    .getQueryResultFromFile(fileName, Integer.valueOf(page), Integer.valueOf(pageSizeString));
            properties.put("success", true);
            properties.put("data", result);
        } catch (Exception e) {
            properties.put("success", false);
            properties.put("errmsg", "progress not finish or progress internal error");
            return createCommonEvent(properties);
        }
        return createCommonEvent(properties);
    }

    public static LogsReadContext asLogsQueyContext(Event event) {
        String requestId = EventUtil.getRequestId(event);
//    logger.info("action task: requestId is {}, state is {}.", requestId, state.toString());
        Map<String, Object> propertyValues = event.getPropertyValues();
        List<String> showCols = (List<String>) propertyValues.get("show_cols");
        List<Map<String, Object>> terms = (List<Map<String, Object>>) propertyValues.get("terms");
        String eventName = EventUtil.getEventName(event);
        Map<String, Object> showProperties = new HashMap<>();
        showCols.forEach(show -> showProperties.put(show, ""));
        Map<String, PropertyFilter> nameFilters = FilterGenerator.generateFilters(terms);
        String fileName = IOUtil.generateFileName(requestId);
        QueryCSVWriter writer = null;
        try {
            writer = IOUtil.createWriter(fileName, showCols);
        } catch (IOException exception) {
            exception.printStackTrace();
            writer = null;
        }
        long startTime = (long) event.getPropertyValues().get("fromtime");
        long endTime = (long) event.getPropertyValues().get("endtime");
        LogsReadContextBuilder builder = new LogsReadContextBuilder(eventName, startTime, endTime);
        builder.filters(nameFilters);
        builder.properties(showProperties);
        builder.writer(writer);
        return builder.build();
    }
}
