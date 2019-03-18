package com.threathunter.persistent.core.io;

import au.com.bytecode.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class QueryCSVWriter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryCSVWriter.class);

  private CSVWriter csvWriter;
  private String[] properties;

  private File targetFile;
  private File tempFile;

  public QueryCSVWriter(final String filePath, final List<String> showCols) throws IOException {
    this.targetFile = new File(filePath);
    this.tempFile = new File(String.format("%s.tmp", filePath));
    if (targetFile.exists()) {
      targetFile.delete();
    }
    if (tempFile.exists()) {
      tempFile.delete();
    }
    this.csvWriter = new CSVWriter(new BufferedWriter(new FileWriter(tempFile)));
    this.properties = (String[]) showCols.toArray(new String[showCols.size()]);
    this.csvWriter.writeNext(properties);
  }

  public void writeQueryData(final Map<String, Object> mapData) {
    String[] nextLine = getNextLine(mapData);
    this.csvWriter.writeNext(nextLine);
  }

  public void close() {
    try {
      this.csvWriter.flush();
    } catch (Exception e) {
      LOGGER.error("flush csv data error", e);
    }
    try {
      this.csvWriter.close();
    } catch (Exception e) {
      LOGGER.error("close csv writer error", e);
    }
    try {
      Files.move(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
    } catch (Exception e) {
      LOGGER.error("rename file error", e);
    }
  }

  private String[] getNextLine(final Map<String, Object> map) {
    String[] data = new String[this.properties.length];
    for (int i = 0; i < data.length; i++) {
      data[i] = map.getOrDefault(this.properties[i], "").toString().replaceAll("\n"," ");
    }

    return data;
  }
}
