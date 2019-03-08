package com.threathunter.persistent.core;


import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMeta;
import com.threathunter.model.Property;
import com.threathunter.persistent.core.util.SystemClock;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * There is no synchronized for all the update method.
 * When nebula start, every module is started one by one, it is safe.
 * Make sure there is only one thread to update from remote timely.
 *
 * @since 1.4
 * @author daisy
 */
public class EventSchemaRegister {
    private volatile long updateTimestamp = -1;

    private final List<String> headerKeys;
    private final int version = 1;
    private final Map<String, List<String>> rawVersionHeaderMap;

    private volatile List<EventMeta> metas;
    private volatile Map<String, EventSchema> logSchemaMap;

    private static final EventSchemaRegister INSTANCE = new EventSchemaRegister();

    public static EventSchemaRegister getInstance() {
        return INSTANCE;
    }

    private EventSchemaRegister() {
        this.headerKeys = new ArrayList<>();
        this.headerKeys.add("c_ip");
        this.headerKeys.add("uid");
        this.headerKeys.add("did");
        this.headerKeys.add("page");

        this.rawVersionHeaderMap = new HashMap<>();
        this.rawVersionHeaderMap.put(this.version + "", this.headerKeys);
    }

    public Map<String, EventSchema> getSchemaMap() {
        return this.logSchemaMap;
    }
    public List<String> getLogKeys() {
        return headerKeys;
    }

    public List<EventMeta> getMetas() {
        return this.metas;
    }

    public Map<String, List<String>> getRawVersionHeaderMap() {
        return this.rawVersionHeaderMap;
    }

    public void update(InputStream schemaFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<EventMeta> metas = new ArrayList<>();
        List<Object> schemas = mapper.readValue(
                schemaFile, List.class);
        schemas.forEach(s -> metas.add(BaseEventMeta.from_json_object(s)));
        update(metas);
    }

    public void update(String schemasPath) throws IOException {
        File schema = new File(schemasPath);
        if (schema.exists()) {
            update(new FileInputStream(schema));
        } else {
            update(Thread.currentThread().getContextClassLoader().getResourceAsStream(schemasPath));
        }
    }

    public long getUpdateTimeStamp() {
        return this.updateTimestamp;
    }

    public int getVersion() {
        return this.version;
    }

    public void update(List<EventMeta> metas) {
        if (metas != null) {
            this.logSchemaMap = getSchemaMapFromEventMetas(metas);
            this.metas = metas;
        }
        this.updateTimestamp = SystemClock.getCurrentTimestamp();
    }

    private Map<String, EventSchema> getSchemaMapFromEventMetas(List<EventMeta> metas) {
        Map<String, EventSchema> schemaMap = new HashMap<>();
        metas.forEach(m -> schemaMap.put(m.getName(),
                new SimpleEventSchema(m.getName(), m.getProperties())));
        return schemaMap;
    }

    public static class SimpleEventSchema implements EventSchema {
        private String schemaType;
        private List<String> properties;
        private List<Property> typedProperties;

        public SimpleEventSchema(String type, List<Property> properties) {
            this.schemaType = type;
            this.properties = new ArrayList<>();
            properties.forEach(p -> this.properties.add(p.getName()));
            this.typedProperties = properties;
        }

        @Override
        public List<String> getBodyProperties() {
            return this.properties;
        }

        @Override
        public List<Property> getProperties() {
            return typedProperties;
        }

        @Override
        public String getLogType() {
            return schemaType;
        }
    }
}
