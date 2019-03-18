package com.threathunter.persistent.core.io;

import au.com.bytecode.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class QueryCSVReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryCSVReader.class);

  private static int COUNT_PER_PAGE = 20;

  private final CSVReader reader;
  public QueryCSVReader(final String filePath) throws FileNotFoundException {
    this.reader = new CSVReader(new BufferedReader(new FileReader(filePath)));
  }

  public List<Map<String, Object>> readLines(final int page ){
    return readLines(page, COUNT_PER_PAGE);
  }

  public List<Map<String, Object>> readLines(final int page, final int pageSize) {
    List<Map<String, Object>> list = new ArrayList<>();
    try {
      // get the fields
      String[] names = reader.readNext();

      int pageCount = pageSize == 0 ? COUNT_PER_PAGE : pageSize;
      int skipCount = (page-1) * pageCount;

      while (skipCount > 0) {
        if (reader.readNext() == null) {
          break;
        }
        skipCount--;
      }

      // read need data
      int c = pageCount;
      while (c > 0) {
        String[] next = reader.readNext();
        if (next == null) {
          break;
        }
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < names.length; i++) {
          map.put(names[i], next[i]);
        }
        list.add(map);
        c--;
      }
      return list;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
