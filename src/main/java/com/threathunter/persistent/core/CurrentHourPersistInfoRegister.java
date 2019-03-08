package com.threathunter.persistent.core;

import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMeta;
import com.threathunter.persistent.core.util.SystemClock;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is write by EventWriter, and read by others.
 * Consider make it as private class in EventWriter.
 * @since 1.4
 * @author daisy
 */
public class CurrentHourPersistInfoRegister {
    private volatile Map<String, EventSchema> currentEventLogSchemaMap;
    private volatile List<EventMeta> metas;

    private final List<String> currentEventHeaderKeys;
    private final int version = 1;
    private final Map<String, List<String>> rawVersionHeaderMap;

    private volatile long lastUpdateTimestamp = -1;

    private static final CurrentHourPersistInfoRegister REGISTER = new CurrentHourPersistInfoRegister();

    private CurrentHourPersistInfoRegister() {
        this.currentEventHeaderKeys = new ArrayList<>();
        this.currentEventHeaderKeys.add("c_ip");
        this.currentEventHeaderKeys.add("uid");
        this.currentEventHeaderKeys.add("did");
        this.currentEventHeaderKeys.add("page");

        this.rawVersionHeaderMap = new HashMap<>();
        this.rawVersionHeaderMap.put(this.version + "", this.currentEventHeaderKeys);
    }

    public static CurrentHourPersistInfoRegister getInstance() {
        return REGISTER;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public EventSchema getEventLogSchema(String name) {
        return currentEventLogSchemaMap.get(name);
    }

    public boolean containsSchema(String name) {
        return currentEventLogSchemaMap.containsKey(name);
    }

    public Map<String, EventSchema> getSchemaMap() {
        return this.currentEventLogSchemaMap;
    }

    public List<String> getCurrentEventHeaderKeys() {
        return currentEventHeaderKeys;
    }

    public Map<String, List<String>> getRawVersionHeaderMap() {
        return rawVersionHeaderMap;
    }

    public List<EventMeta> getEventMetas() {
        return this.metas;
    }

    public int getVersion() {
        return this.version;
    }

    public void updateFromLogSchemaRegister() {
        if (lastUpdateTimestamp < EventSchemaRegister.getInstance().getUpdateTimeStamp()) {
            this.currentEventLogSchemaMap = EventSchemaRegister.getInstance().getSchemaMap();
            this.metas = EventSchemaRegister.getInstance().getMetas();
            this.lastUpdateTimestamp = SystemClock.getCurrentTimestamp();
        }
    }

    public void update(File schemaFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> schemas = mapper.readValue(
                schemaFile, List.class);
        List<EventMeta> metas = new ArrayList<>();
        schemas.forEach(s -> metas.add(BaseEventMeta.from_json_object(s)));
        update(metas);
    }

    public void update(String schemasPath) throws IOException {
        update(new File(schemasPath));
    }

    private void update(List<EventMeta> eventMetas) {
        this.currentEventLogSchemaMap = getSchemaMapFromEventMeta(eventMetas);
        this.metas = eventMetas;

        this.lastUpdateTimestamp = SystemClock.getCurrentTimestamp();
    }

    private Map<String, EventSchema> getSchemaMapFromEventMeta(List<EventMeta> metas) {
        Map<String, EventSchema> schemaMap = new HashMap<>();
        metas.forEach(((m) -> schemaMap.put(m.getName(),
                new EventSchemaRegister.SimpleEventSchema(m.getName(), m.getProperties()))));

        return schemaMap;
    }
}
