package com.threathunter.persistent.core;

import com.threathunter.model.Event;
import com.threathunter.persistent.core.api.LogsReadContext;
import com.threathunter.persistent.core.api.SequenceReadContext;
import com.threathunter.persistent.core.io.ShardFile;
import com.threathunter.persistent.core.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * 
 */
public class HourDirSequeceRead {

    private final String dir;
    private final SequenceReadContext context;
    Logger logger = LoggerFactory.getLogger(HourDirSequeceRead.class);
    private Long startPoint;
    private Long endPoint;
    private ShardFile[] shards;
    private Map<String, ShardFile> pair;
    private PriorityQueue<EventFilePair> minHeap = new PriorityQueue<EventFilePair>(16, (t1, t2) -> {
        if (t1.event.getTimestamp() < t2.event.getTimestamp()) {
            return -1;
        } else if (t1.event.getTimestamp() > t2.event.getTimestamp()) {
            return 1;
        }
        return t1.event.getName().compareTo(t2.event.getName());
    });

    public HourDirSequeceRead(SequenceReadContext context) {
        try {
            this.dir = context.getDir();
            pair = new HashMap<String, ShardFile>();
            initShardFiles();
            this.context = context;
            CurrentHourPersistInfoRegister.getInstance().update(IOUtil.getPathOfEventsSchema(dir));
        } catch (Exception e) {
            logger.error("contain folder is {}, init error.", context.getDir());
            throw new RuntimeException("dir error", e);
        }
    }

    private void initShardFiles() {
        shards = IOUtil.createShardFiles(dir);
        for (int i = 0; i < 16; i++) {
            pair.put(shards[i].getShardName(), shards[i]);
        }

    }

    public void read() {
        for (ShardFile file : shards) {
            Event event = file.next(context);
            if (event != null) {
                EventFilePair ef = new EventFilePair(event, file);
                if (!minHeap.offer(ef)) {
                    throw new IllegalArgumentException("shard:heap:initial fail. Disk space is full?");
                }

            }
        }
        while (minHeap.size() > 0) {
            EventFilePair ef = minHeap.poll();
            if (ef == null) {
                continue;
            }
            context.addEvent(ef.event);
//            String name = event.getName();
            ShardFile file = ef.file;
            Event internalRow = file.next(context);
            if (internalRow != null) {
                ef.event = internalRow;
                minHeap.offer(ef);
            }
        }
    }

    private class EventFilePair {
        Event event;
        ShardFile file;

        EventFilePair(Event event, ShardFile file) {
            this.event = event;
            this.file = file;
        }
    }
}
