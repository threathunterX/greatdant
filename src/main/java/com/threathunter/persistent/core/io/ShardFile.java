package com.threathunter.persistent.core.io;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;
import com.threathunter.model.Property;
import com.threathunter.persistent.core.CurrentHourPersistInfoRegister;
import com.threathunter.persistent.core.EventReadHelper;
import com.threathunter.persistent.core.KVRow;
import com.threathunter.persistent.core.api.LogsReadContext;
import com.threathunter.persistent.core.api.SequenceReadContext;
import com.threathunter.persistent.core.filter.PropertyFilter;
import com.threathunter.persistent.core.util.BRAFileHelper;
import com.threathunter.persistent.core.util.ConstantsUtil;
import com.threathunter.persistent.core.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.threathunter.persistent.core.util.BRAFileHelper.getFieldBytes;

/**
 * 
 */
public class ShardFile {

    Logger logger = LoggerFactory.getLogger(ShardFile.class);
    private String shardName;
    private BufferedRandomAccessFile file;
    private long currentOffset;

    public ShardFile(String name, String dir) throws IOException {
        shardName = name;
        file = IOUtil.createBAFile(name, dir);
        currentOffset = 0L;
    }

    public KVRow next(LogsReadContext context) {
        try {
            long nextOffset;
            long currentEventTs = 0l;
            while (currentOffset < file.length()) {
                file.seek(currentOffset);
               //  checkValid();
                long entryPos = file.getFilePointer();
                currentOffset = entryPos;
                file.seek(entryPos);
                int remainSize = BRAFileHelper.getNextFieldSize(file, 2);
                int currentSize = remainSize;
                if (!BRAFileHelper.checkCodeValid(file, currentOffset, currentSize)) {
                    return null;
                }

                int checkCodeSize = 8 - (currentSize % 8);
                nextOffset = currentOffset + currentSize + checkCodeSize;
                // check time first
                file.seek(currentOffset + 4);
                currentEventTs = BRAFileHelper.getNextFieldLong(file, 8);
                long startTime = context.getStartPoint();
                long endTime = context.getEndPoint();
                if (context.getStartPoint() > 0) {
                    if (currentEventTs < startTime) {
                        currentOffset = nextOffset;
                        continue;
                    }
                    if (currentEventTs >= endTime) {
                        break;
                    }
                }

                file.seek(currentOffset + 2);
                int jumpSize = BRAFileHelper.getNextFieldSize(file, 2);
                file.seek(currentOffset + 4 + jumpSize);
                int appSize = BRAFileHelper.getNextFieldSize(file, 2);
                // ignore app
                file.seek(file.getFilePointer() + appSize);
                int eventNameSize = BRAFileHelper.getNextFieldSize(file, 2);
                String currentEventName = BRAFileHelper.getNextFieldString(file, eventNameSize);
                if (!currentEventName.equals(context.getEventName())) {
                    currentOffset = nextOffset;
                    continue;
                }
                long propertyStartOffset = file.getFilePointer();

                int propertyCount = 0;
                Map<String, Object> queryProperties = context.cloneQueryProperties();
                if (queryProperties.containsKey("id")) {
                    file.seek(currentOffset + 13);
                    queryProperties
                            .put("id", new ObjectId(getFieldBytes(file, 12)).toHexString());
                    propertyCount++;
                }
                if (queryProperties.containsKey("pid")) {
                    file.seek(currentOffset + 25);
                    queryProperties
                            .put("pid", new ObjectId(getFieldBytes(file, 12)).toHexString());
                    propertyCount++;
                }
                if (queryProperties.containsKey("timestamp")) {
                    queryProperties.put("timestamp", currentEventTs);
                    propertyCount++;
                }

                boolean find = true;
                file.seek(propertyStartOffset);
                for (Property m : CurrentHourPersistInfoRegister.getInstance()
                        .getEventLogSchema(context.getEventName()).getProperties()) {
                    String field = m.getName();
                    int fieldSize = BRAFileHelper.getNextFieldSize(file, 2);
                    Object fieldValue = BRAFileHelper.parseField(file, m.getType().getCode(), fieldSize);
                    Map<String, PropertyFilter> filters = context.getNamedFilters();
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
                currentOffset = nextOffset;
                if (!find) {
                    continue;
                }
                currentOffset = nextOffset;
                KVRow row = new KVRow(currentEventTs, getShardName(), queryProperties);
                return row;
            }
        } catch (IOException e) {
            logger.info("ShardFile:Context:currentOffSet:" + currentOffset, e);
        }
        return null;
    }

    public Event next(SequenceReadContext context) {
        try {
            long nextOffset;
            long currentEventTs = 0l;
            while (currentOffset < file.length()) {
                file.seek(currentOffset);
//                checkValid();
                long entryPos = file.getFilePointer();
                currentOffset = entryPos;
                file.seek(entryPos);
                int remainSize = BRAFileHelper.getNextFieldSize(file, 2);
                int currentSize = remainSize;
                if (!BRAFileHelper.checkCodeValid(file, currentOffset, currentSize)) {
                    return null;
                }

                int checkCodeSize = 8 - (currentSize % 8);
                nextOffset = currentOffset + currentSize + checkCodeSize;
                // check time first
                file.seek(currentOffset + 4);
                currentEventTs = BRAFileHelper.getNextFieldLong(file, 8);
                long startTime = context.getStartPoint();
                long endTime = context.getEndPoint();
                if (context.getStartPoint() > 0) {
                    if (currentEventTs < startTime) {
                        currentOffset = nextOffset;
                        continue;
                    }
                    if (currentEventTs >= endTime) {
                        break;
                    }
                }

                Event event=new Event();
                EventReadHelper helper=new EventReadHelper();
                currentOffset=helper.readEvent(file,event,currentOffset,null,"");
                return event;
            }
        } catch (IOException e) {
            logger.info("ShardFile:Context:currentOffSet:" + currentOffset, e);
        }
        return null;
    }

  /*  private void checkValid() {
        byte[] entryName = BRAFileHelper.getFieldBytes(file, ConstantsUtil.getEventEntryBytes().length);
        if (ConstantsUtil.EVENT_ENTRY.equals(new String(entryName))) {
            try {
                int endFlag = file.read();
                if (endFlag==1)
                    return;
            } catch (IOException e) {
                logger.error(file.getPath() + " read error:failed.", e);
                //fail
                throw new RuntimeException();
            }
        }
        seekToNext();
    }*/

//    private void seekToNext() {
//        byte[] encoded = ConstantsUtil.getEventEntryBytes();
//        byte[] subEncoded= Arrays.copyOfRange(encoded,1,encoded.length);
//        int b;
//        try {
//            while((b = file.read()) !=-1){
//               if(encoded[0]==b){
//                 long pos = file.getFilePointer();
//                 int length=encoded.length-1;
//                 byte[] remain=new byte[length];
//                 file.read(remain,0,length);
//                 if(Arrays.equals(subEncoded,remain)){
//                     int endFlag = file.read();
//                     if (endFlag==1)
//                         return;
//                 }
//                 file.seek(pos);
//               }
//            }
//        } catch (IOException e) {
//            logger.error(file.getPath() + " read error:failed.", e);
//            //fail
//            throw new RuntimeException();
//        }
//
//
//    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }
}
