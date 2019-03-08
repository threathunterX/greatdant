package com.threathunter.persistent.core;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;
import com.threathunter.model.Property;
import com.threathunter.persistent.core.filter.PropertyFilter;
import com.threathunter.persistent.core.io.BufferedRandomAccessFile;
import com.threathunter.persistent.core.util.BytesDecoder;
import com.threathunter.persistent.core.util.ConstantsUtil;
import com.google.common.primitives.Ints;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by daisy on 16-4-16.
 */
public class EventReadHelper {
    private static final Map<String, Decoder> DECODER_MAP;

    private interface Decoder {
        Object decodeField(BufferedRandomAccessFile file, int size);
    }
    public static final int NO_MATCH = -1;
    public static final int ERR_CHECK_CODE = -2;
    public static final int ERR_READ = -3;

    public EventReadHelper() {

    }

    static {
        DECODER_MAP = new HashMap<>();

        DECODER_MAP.put("int", (file, len) -> getNextFieldInteger(file, len));
        DECODER_MAP.put("long", (file, len) -> getNextFieldLong(file, len));
        DECODER_MAP.put("bool", (file, len) -> getNextFieldBoolean(file, len));
        DECODER_MAP.put("string", (file, len) -> getNextFieldString(file, len));
        DECODER_MAP.put("double", (file, len) -> getNextFieldDouble(file, len));
    }

    public List<Event> readEvents(final BufferedRandomAccessFile file) {
        // scan event from file
        List<Event> result  = new ArrayList<>();
        try {
            long offset = 0;
            long size = file.length();
            while (offset < size) {
                Event e = new Event();
                offset = readEvent(file, e, offset);
                // fail to read a event
                if (offset < 0)
                    break;
                result.add(e);
            }
        } catch (IOException e) {
        }

        return result;
    }


    public List<Event> readEvents(final BufferedRandomAccessFile file, String headerField, String key) {
        List<Event> result = new ArrayList<>();
        try {
            long offset = 0;
            long size = file.length();
            while (offset < size) {
                Event e = new Event();
                offset = readEvent(file, e, offset, headerField, key);
                // fail to read a event
                if (offset < 0)
                    break;
                result.add(e);
            }
        } catch (IOException e) {
        }

        return result;
    }

    public long readEvent(final BufferedRandomAccessFile file, Event event, long offset) throws IOException {
        return readEvent(file, event, offset, "", "");
    }

    public long readEvent(final BufferedRandomAccessFile file, Event event, long offset, String headerField, String key) throws IOException {
        return readEvent(file, event, offset, headerField, key, -1, -1);
    }

    public long readEvent(final BufferedRandomAccessFile file, final Event event, long offset, String headerField, String key, long startTime, long endTime) throws IOException {
        long currentOffset = offset;
        long nextOffset;
        while (currentOffset < file.length()) {
            file.seek(currentOffset);
            int remainSize = getNextFieldSize(file, 2);
            int currentSize = remainSize;
            if (!checkCodeValid(file, currentOffset, currentSize)) {
                return ERR_CHECK_CODE;
            }

            int checkCodeSize = 8 - (currentSize % 8);
            nextOffset = currentOffset + currentSize + checkCodeSize;
            // check time first
            file.seek(currentOffset + 4);
            long currentEventTs = getNextFieldLong(file, 8);
            if (startTime > 0) {
                if (currentEventTs < startTime) {
                    currentOffset = nextOffset;
                    continue;
                }
                if (currentEventTs >= endTime) {
                    break;
                }
            }

            if (headerField != null && !headerField.isEmpty()) {
                if (!matchKey(file, currentOffset, key, headerField)) {
                    currentOffset = nextOffset;
                    continue;
                }
            }

            // get id and pid
            file.seek(currentOffset + 13);
            event.setId(new ObjectId(getFieldBytes(file, 12)).toHexString());
            event.setPid(new ObjectId(getFieldBytes(file, 12)).toHexString());

            // get jump size, directly jump to app fellow to get content
            file.seek(currentOffset + 2);
            int jumpSize = getNextFieldSize(file, 2);

            event.setTimestamp(currentEventTs);
            // jump to get app and other field to fill the event
            file.seek(currentOffset + 4 + jumpSize);
            remainSize -= (4 + jumpSize);
            int appSize = getNextFieldSize(file, 2);
            event.setApp(getNextFieldString(file, appSize));
            remainSize -= (appSize + 2);
            int nameSize = getNextFieldSize(file, 2);
            String eventName = getNextFieldString(file, nameSize);
            event.setName(eventName);
            remainSize -= (nameSize + 2);

            Map<String, Object> properties = new HashMap<>();

            for (Object m : CurrentHourPersistInfoRegister.getInstance().getEventLogSchema(eventName).getProperties()) {
                assert m instanceof Property;
                Property p=(Property) m;
//                Map<String, String>   map = (Map<String, String>)m;
                String field=p.getName();

                int fieldSize = getNextFieldSize(file, 2);
                remainSize -= 2;
                Object fieldValue = parseField(file, p.getType().getCode(), fieldSize);
                properties.put(field, fieldValue);
                remainSize -= fieldSize;
                if (remainSize < 0) {
                    return ERR_READ;
                }
            }
            if (remainSize > 0) {
                event.setValue((Double) parseField(file, "double", 8));
                remainSize -= 8;
            }

            if (remainSize != 0) {
                return ERR_READ;
            }

            event.setPropertyValues(properties);
            return nextOffset;
        }
        return NO_MATCH;
    }

    public long getEventData(final BufferedRandomAccessFile file, long offset, String headerField, String key, long startTime, long endTime, Map<String, Object> properties) throws IOException {
        long currentOffset = offset;
        long nextOffset;
        while (currentOffset < file.length()) {
            file.seek(currentOffset);
            int currentSize = getNextFieldSize(file, 2);
            if (!checkCodeValid(file, currentOffset, currentSize)) {
                return ERR_CHECK_CODE;
            }

            int checkCodeSize = 8 - (currentSize % 8);
            nextOffset = currentOffset + currentSize + checkCodeSize;
            // check time first
            file.seek(currentOffset + 4);
            long currentEventTs = getNextFieldLong(file, 8);
            if (startTime > 0) {
                if (currentEventTs < startTime) {
                    currentOffset = nextOffset;
                    continue;
                }
                if (currentEventTs >= endTime) {
                    break;
                }
            }

            if (key != null && !key.isEmpty()) {
                if (!matchKey(file, currentOffset, key, headerField)) {
                    currentOffset = nextOffset;
                    continue;
                }
            }

            int propertyCount = 0;
            // check if need id and pid
            if (properties.containsKey("id")) {
                file.seek(currentOffset + 13);
                properties.put("id", new ObjectId(getFieldBytes(file, 12)).toHexString());
                propertyCount++;
            }
            if (properties.containsKey("pid")) {
                file.seek(currentOffset + 25);
                properties.put("pid", new ObjectId(getFieldBytes(file, 12)).toHexString());
                propertyCount++;
            }

            // get jump size, directly jump to app fellow to get content
            file.seek(currentOffset + 2);
            int jumpSize = getNextFieldSize(file, 2);

            // jump to get app and other field to fill the event
            file.seek(currentOffset + 4 + jumpSize);
            int appSize = getNextFieldSize(file, 2);
            if (properties.containsKey("app")) {
                properties.put("app", getNextFieldString(file, appSize));
                propertyCount++;
            } else {
                file.seek(file.getFilePointer() + appSize);
            }

            int nameSize = getNextFieldSize(file, 2);
            String eventName = getNextFieldString(file, nameSize);
            if (properties.containsKey("name")) {
                properties.put("name", eventName);
                propertyCount++;
            }
            // get properties
            if (properties.containsKey("timestamp")) {
                properties.put("timestamp", currentEventTs);
                propertyCount++;
            }
            for (Property m : CurrentHourPersistInfoRegister.getInstance().getEventLogSchema(eventName).getProperties()) {
                String field =m.getName();

                int fieldSize = getNextFieldSize(file, 2);
                if (properties.containsKey(field)) {
                    properties.put(field, parseField(file,m.getType().getCode(), fieldSize));
                    propertyCount++;
                } else {
                    file.seek(file.getFilePointer() + fieldSize);
                }
                if (propertyCount >= properties.size()) {
                    break;
                }
            }

            return nextOffset;
        }
        return NO_MATCH;
    }

    public long queryEvent(final BufferedRandomAccessFile file, final Event event, long offset, String eventName, Map<String, PropertyFilter> namedFilters, long startTime, long endTime) throws IOException {
        long currentOffset = offset;
        long nextOffset;
        while (currentOffset < file.length()) {
            file.seek(currentOffset);
            int remainSize = getNextFieldSize(file, 2);
            int currentSize = remainSize;
            if (!checkCodeValid(file, currentOffset, currentSize)) {
                return ERR_CHECK_CODE;
            }

            int checkCodeSize = 8 - (currentSize % 8);
            nextOffset = currentOffset + currentSize + checkCodeSize;
            // check time first
            file.seek(currentOffset + 4);
            long currentEventTs = getNextFieldLong(file, 8);
            if (startTime > 0) {
                if (currentEventTs < startTime) {
                    currentOffset = nextOffset;
                    continue;
                }
                if (currentEventTs >= endTime) {
                    break;
                }
            }
            event.setTimestamp(currentEventTs);
            file.seek(currentOffset + 2);
            int jumpSize = getNextFieldSize(file, 2);
            file.seek(currentOffset + 4 + jumpSize);
            int appSize = getNextFieldSize(file, 2);
            event.setApp(getNextFieldString(file, appSize));
            // get event name
            int eventNameSize = getNextFieldSize(file, 2);
            String currentEventName = getNextFieldString(file, eventNameSize);
            if (!currentEventName.equals(eventName)) {
                currentOffset = nextOffset;
                continue;
            }
            event.setName(currentEventName);

//            long propertyStartOffset = file.getFilePointer();
            boolean find = true;
            for (Object m : CurrentHourPersistInfoRegister.getInstance().getEventLogSchema(eventName).getProperties()) {
                Map<String, String> map = (Map<String, String>) m;
                String field = map.get("name").toString();
                int fieldSize = getNextFieldSize(file, 2);
                Object fieldValue = parseField(file, map.get("type"), fieldSize);
                if (namedFilters.containsKey(field)) {
                    if (!namedFilters.get(field).match(fieldValue.toString())) {
                        find = false;
                        break;
                    }
                }
                event.getPropertyValues().put(field, fieldValue);
            }
            if (!find) {
                currentOffset = nextOffset;
                continue;
            } else {
                if (file.getFilePointer() - currentOffset < currentSize) {
                    event.setValue((Double) parseField(file, "double", 8));
                }
                file.seek(currentOffset + 13);
                event.setId(new ObjectId(getFieldBytes(file, 12)).toHexString());
                event.setPid(new ObjectId(getFieldBytes(file, 12)).toHexString());
            }
            return nextOffset;
        }
        return NO_MATCH;
    }

    public long queryEventData(final BufferedRandomAccessFile file, Map<String, Object> queryProperties, long offset, String eventName, Map<String, PropertyFilter> filters, long startTime, long endTime) throws IOException {
        long currentOffset = offset;
        long nextOffset;
        while (currentOffset < file.length()) {
            file.seek(currentOffset);
            int remainSize = getNextFieldSize(file, 2);
            int currentSize = remainSize;
            if (!checkCodeValid(file, currentOffset, currentSize)) {
                return ERR_CHECK_CODE;
            }

            int checkCodeSize = 8 - (currentSize % 8);
            nextOffset = currentOffset + currentSize + checkCodeSize;
            // check time first
            file.seek(currentOffset + 4);
            long currentEventTs = getNextFieldLong(file, 8);
            if (startTime > 0) {
                if (currentEventTs < startTime) {
                    currentOffset = nextOffset;
                    continue;
                }
                if (currentEventTs >= endTime) {
                    break;
                }
            }

            file.seek(currentOffset + 2);
            int jumpSize = getNextFieldSize(file, 2);
            file.seek(currentOffset + 4 + jumpSize);
            int appSize = getNextFieldSize(file, 2);
            // ignore app
            file.seek(file.getFilePointer() + appSize);
            int eventNameSize = getNextFieldSize(file, 2);
            String currentEventName = getNextFieldString(file, eventNameSize);
            if (!currentEventName.equals(eventName)) {
                currentOffset = nextOffset;
                continue;
            }
            long propertyStartOffset = file.getFilePointer();

            int propertyCount = 0;
            if (queryProperties.containsKey("id")) {
                file.seek(currentOffset + 13);
                queryProperties.put("id", new ObjectId(getFieldBytes(file, 12)).toHexString());
                propertyCount++;
            }
            if (queryProperties.containsKey("pid")) {
                file.seek(currentOffset + 25);
                queryProperties.put("pid", new ObjectId(getFieldBytes(file, 12)).toHexString());
                propertyCount++;
            }
            if (queryProperties.containsKey("timestamp")) {
                queryProperties.put("timestamp", currentEventTs);
                propertyCount++;
            }

            boolean find = true;
            file.seek(propertyStartOffset);
            for (Object m : CurrentHourPersistInfoRegister.getInstance().getEventLogSchema(eventName).getProperties()) {
                Map<String, String> map = (Map<String, String>) m;
                String field = map.get("name").toString();
                int fieldSize = getNextFieldSize(file, 2);
                Object fieldValue = parseField(file, map.get("type"), fieldSize);
                if (filters.containsKey(field)) {
                    if (!filters.get(field).match(fieldValue.toString())) {
                        find = false;
                        break;
                    }
                }
                if (queryProperties.containsKey(field)) {
                    queryProperties.put(field, fieldValue);
                    propertyCount++;
                }
                if (propertyCount >= queryProperties.size()) {
                    break;
                }
            }
            if (!find) {
                currentOffset = nextOffset;
                continue;
            }
            return nextOffset;
        }
        return NO_MATCH;
    }

    public static BufferedRandomAccessFile getBufferedRandomAccessFile(String persistentPath, long fromHourSlot, int shard) throws IOException {
        String hourDir = new DateTime(fromHourSlot).toString("yyyyMMddHH");
        String tmpFileName = String.format("%s/%s/%s.%s/%d", persistentPath, hourDir, ConstantsUtil.PERSISTENT_EVENT_DIR,
                ConstantsUtil.PERSISTENT_TEMP_SUFFIX, shard);

        BufferedRandomAccessFile file;

        if (!new File(tmpFileName).exists()) {
            file = new BufferedRandomAccessFile(
                    String.format("%s/%s/%s/%d", persistentPath, hourDir, ConstantsUtil.PERSISTENT_EVENT_DIR, shard), "r");
        } else {
            file = new BufferedRandomAccessFile(tmpFileName, "r");
        }

        return file;
    }

    private static Object parseField(BufferedRandomAccessFile file, String fieldType, int size) {
        if (size == 0) {
            return null;
        }
        if (!DECODER_MAP.containsKey(fieldType)) {
            return null;
        }
        return DECODER_MAP.get(fieldType).decodeField(file, size);
    }

    private static Integer getNextFieldSize(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return 0;
        }
        byte[] sb = new byte[4];
        sb[0] = 0;
        sb[1] = 0;
        byte[] b = getFieldBytes(file, 2);
        sb[2] = b[0];
        sb[3] = b[1];

        return Ints.fromByteArray(sb);
    }

    private static Integer getNextFieldInteger(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 8);
        return BytesDecoder.getIntFromBytes64(b);
    }

    private static Long getNextFieldLong(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 8);
        return BytesDecoder.getLongFromBytes64(b);
    }

    private static Double getNextFieldDouble(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 8);
        return BytesDecoder.getDoubleFromBytes64(b);
    }

    private static Boolean getNextFieldBoolean(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 1);
        return b[0] > 0;
    }

    private static String getNextFieldString(BufferedRandomAccessFile file, int length) {
        byte[] b = getFieldBytes(file, length);
        return BytesDecoder.getStringFromBytesString(b);
    }

    private static byte[] getFieldBytes(BufferedRandomAccessFile file, int length) {
        byte[] b = new byte[length];

        try {
            int offset = 0;
            while (offset < length) {
                int retSize = file.read(b, offset, length - offset);
                if (retSize < 0)
                    break;

                offset += retSize;
            }
        } catch (IOException e) {
            return new byte[0];
        }

        return b;
    }

    private boolean matchKey(BufferedRandomAccessFile file, long currentOffset, String key, String headerField) throws IOException {
        // check if match the key
        // 37 is from:
        // 2 for event size, 2 for jump size, 8 for timestamp, 1 for version, 12 for id, 12 for pid
        // then we reach the header part
        file.seek(currentOffset + 37);

        int headerFieldIndex = CurrentHourPersistInfoRegister.getInstance().getCurrentEventHeaderKeys().indexOf(headerField);
        for (int i = 0; i < CurrentHourPersistInfoRegister.getInstance().getCurrentEventHeaderKeys().size(); i++) {
            if (i > headerFieldIndex) {
                return false;
            }
            int keySize = getNextFieldSize(file, 2);
            if (i < headerFieldIndex) {
                file.seek(file.getFilePointer() + keySize);
            } else {
                String eventKey = getNextFieldString(file, keySize);
                if (eventKey.equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean checkCodeValid(BufferedRandomAccessFile file, long currentOffset, int eventSize) throws IOException {
        int checkCodeSize = 8 - (eventSize % 8);
        file.seek(currentOffset + eventSize);

        byte[] checkBytes = getFieldBytes(file, checkCodeSize);
        for (byte b : checkBytes) {
            if (b != ConstantsUtil.CHECK_CODE) {
                return false;
            }
        }
        return true;
    }
}
