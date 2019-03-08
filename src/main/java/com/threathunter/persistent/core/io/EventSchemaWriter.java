package com.threathunter.persistent.core.io;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * When close a log file, write the log schema into file in the close thread.
 *
 * @since 1.4
 * @author daisy
 */
public class EventSchemaWriter {
    private final static EventSchemaWriter writer = new EventSchemaWriter();
    public static EventSchemaWriter getInstance() {
        return writer;
    }

    public void writeObjectToFile(Object object, String outputFile) throws IOException {
        String json = getPrettyJson(object);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outputFile), Charset.defaultCharset()
        ));

        writer.write(json);
        writer.close();
    }

    public void writeObjectToFile(Object object, File outputFile) throws IOException {
        String json = getPrettyJson(object);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outputFile), Charset.defaultCharset()
        ));

        writer.write(json);
        writer.close();
    }

    private String getPrettyJson(Object object) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(object);
    }

    public static void main(String[] args) throws IOException {
        Map<String, String> map = new HashMap<>();
        map.put("string1", "value1");
        map.put("string2", "value2");
        map.put("string3", "value3");
        EventSchemaWriter writer = new EventSchemaWriter();
        writer.writeObjectToFile(writer.getPrettyJson(map), "/Users/threathunter-dev/Desktop/schema.json");
    }
}
