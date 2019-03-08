package com.threathunter.persistent.core;

import com.threathunter.persistent.core.api.LogsReadContext;
import com.threathunter.persistent.core.io.ShardFile;
import com.threathunter.persistent.core.util.IOUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Created by yy on 17-9-15.
 */
public class HourDirLogsRead implements LogsRead {

  private final String dir;
  private Long startPoint;
  private Long endPoint;
  private ShardFile[] shards;
  private Map<String, ShardFile> pair;

  private PriorityQueue<KVRow> minHeap = new PriorityQueue<KVRow>(16, (t1, t2) -> {
    return t1.compareTo(t2);
  });

  public HourDirLogsRead(String dir) {
    this.dir = dir;
    pair = new HashMap<String, ShardFile>();
    initShardFiles();
  }

  private void initShardFiles() {
    shards = IOUtil.createShardFiles(dir);
    for (int i = 0; i < 16; i++) {
      pair.put(shards[i].getShardName(), shards[i]);
    }
  }

  @Override
  public void read(LogsReadContext context) {
    for (ShardFile file : shards) {
      KVRow row = file.next(context);
      if (row != null) {
        if (!minHeap.offer(row)) {
          throw new IllegalArgumentException("shard:heap:initial fail. Disk space is full?");
        }
      }
    }
    while (minHeap.size() > 0) {
      KVRow row = minHeap.poll();
      if (row == null) {
        continue;
      }
      context.addrow(row);
      String name = row.getName();
      ShardFile file = pair.get(name);
      KVRow internalRow = file.next(context);
      if (internalRow != null) {
        minHeap.offer(internalRow);
      }
    }

  }


}
