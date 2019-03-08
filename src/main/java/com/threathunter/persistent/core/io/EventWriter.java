package com.threathunter.persistent.core.io;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;
import com.threathunter.persistent.core.util.BytesEncoderDecoder;
import com.threathunter.persistent.core.CurrentHourPersistInfoRegister;
import com.threathunter.persistent.core.util.ConstantsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EventWriter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(EventWriter.class);
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private volatile String currentPath = "";
    // these data are not thread safe.
    private volatile BufferedRandomAccessFile[] bufWriteFiles;
    private volatile boolean running = true;
    private final Object lock = new Object();

    private int logShard;


    public EventWriter() {
    }

    public void initial(String path, int shard) {
        this.logShard = shard;
        updateLogPath(path);
    }

    public void flushAll() {
        if (bufWriteFiles != null) {
            for (BufferedRandomAccessFile file : bufWriteFiles) {
                try {
                    file.flush();
                } catch (IOException e) {
                    logger.error("flush error, file: " + file.getPath(), e);
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closeLogFile(this.bufWriteFiles, currentPath);
            this.bufWriteFiles = null;
            running = false;
        }
    }

    private BufferedRandomAccessFile[] openNewLogFiles(String newPath) {
        String tempFileName = String.format("%s/%s.%s", newPath, ConstantsUtil.PERSISTENT_EVENT_DIR, ConstantsUtil.PERSISTENT_TEMP_SUFFIX);
        File tempFile = new File(tempFileName);
        String fileName = String.format("%s/%s", newPath, ConstantsUtil.PERSISTENT_EVENT_DIR);
        File file = new File(fileName);
        synchronized (lock) {
            if (!running)
                return new BufferedRandomAccessFile[this.logShard];
            if (file.exists()) {
                if (!file.isDirectory()) {
                    String errMsg = String.format("log directory %s has already been created as a file", fileName);
                    logger.error(errMsg);
                    throw new RuntimeException(errMsg);
                } else {
                    try {
                        Files.move(file.toPath(), tempFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                        logger.info("successfully renamed the log data {} to temperate data {}", fileName, tempFileName);
                    } catch (IOException e) {
                        String errMsg = "fail to rename to the temperate file " + tempFileName;
                        logger.error(errMsg, e);
                        throw new RuntimeException(errMsg, e);
                    }
                }
            } else {
                logger.info("will create log directory {}", tempFileName);
                if (tempFile.mkdirs()) {
                    logger.info("successfully create log directory {}", tempFileName);
                }

                if (!tempFile.exists()) {
                    String errMsg = "fail to create log directory " + tempFileName;
                    logger.error(errMsg);
                    throw new RuntimeException(errMsg);
                }
            }
            BufferedRandomAccessFile[] result = new BufferedRandomAccessFile[this.logShard];
            for (int i = 0; i < this.logShard; i++) {
                String logFileName = String.format("%s/%d", tempFileName, i);
                try {
                    result[i] = new BufferedRandomAccessFile(logFileName, "rw");
                    result[i].seek(result[i].length());
                } catch (IOException e) {
                    logger.error("fail to create the log file " + logFileName, e);
                }
            }

            return result;
        }
    }

    private void closeLogFile(BufferedRandomAccessFile[] files, String oldPath) {
        if (files == null || files.length == 0) {
            return;
        }

        for (int i = 0; i < files.length; i++) {
            BufferedRandomAccessFile f = files[i];
            if (f == null) {
                continue;
            }
            String fileName = f.getPath();
            try {
                f.close();
            } catch (IOException e) {
                logger.error("fail to close file " + fileName, e);
            }
        }

        String fileName = String.format("%s/%s.%s", oldPath, ConstantsUtil.PERSISTENT_EVENT_DIR, ConstantsUtil.PERSISTENT_TEMP_SUFFIX);
        File file = new File(fileName);

        String newFileName = String.format("%s/%s", oldPath, ConstantsUtil.PERSISTENT_EVENT_DIR);
        File newFile = new File(newFileName);
        if (file.exists()) {
            try {
                Files.move(file.toPath(), newFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                logger.info("successfully renamed the log file {}", fileName);
            } catch (IOException e) {
                logger.error("fail to rename the log file " + fileName, e);
            }
        } else {
            logger.error("no log file with name {} is found when closing the old log directory", fileName);
        }
    }

    public boolean updateLogPath(String newPath) {
        if (newPath.equals(this.currentPath)) {
            return true;
        }
        if (this.bufWriteFiles != null) {
            final BufferedRandomAccessFile[] oldFiles = this.bufWriteFiles;
            final String oldPath = currentPath;
            new Thread(() -> closeLogFile(oldFiles, oldPath)).start();
        }
        // TODO flush check?

        try {
            this.bufWriteFiles = openNewLogFiles(newPath);
        } catch (Exception e) {
            this.bufWriteFiles = new BufferedRandomAccessFile[logShard];
            logger.error("failed to update to new log path", e);
            return false;
        }
        this.currentPath = newPath;

        return true;
    }


    /**
     * Log event to file
     * @param event
     * @return
     */
    public long writeEventLog(Event event, int shard) {
        if (this.bufWriteFiles[shard] != null) {
            return writeBufEventLog(this.bufWriteFiles[shard], event);
        } else {
            // TODO try to reopen file
            throw new RuntimeException("log file is not found");
        }
    }

    private long writeBufEventLog(BufferedRandomAccessFile shardFile, Event event) {
        // name infer is a httplog, loginlog, redistlog or httpstaticlog
        String logType = event.getName();
        if (!CurrentHourPersistInfoRegister.getInstance().containsSchema(logType)) {
            return -1;
        }
        Map<String, Object> properties = event.getPropertyValues();

        // TODO may need to seek again if error occur during writing
        try {
            long pos = shardFile.getFilePointer();
            // the whole record size
            shardFile.write(BytesEncoderDecoder.encode16(0));
            // the jumpper from timestamp to size of app, 16 bits
            shardFile.write(BytesEncoderDecoder.encode16(0));
            // the end offset jumpper will reach
            long jumpperEndPos;

            shardFile.write(BytesEncoderDecoder.encode64(event.getTimestamp())); //timestamp
            shardFile.write(BytesEncoderDecoder.encode8(CurrentHourPersistInfoRegister.getInstance().getVersion())); // version 0
            shardFile.write(new ObjectId(event.getId()).toByteArray()); // id
            String pid = event.getPid();
            byte[] pidBytes;
            if (pid == null || pid.equals("")) {
                pidBytes = ObjectId.ZEROID.toByteArray();
            } else {
                pidBytes = new ObjectId(event.getPid()).toByteArray();
            }
            shardFile.write(pidBytes); // pid

            // write header keys
            for (String key : CurrentHourPersistInfoRegister.getInstance().getCurrentEventHeaderKeys()) {
                try {
                    writeStrToBuffer(shardFile, (String) event.getPropertyValues().get(key));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            jumpperEndPos = shardFile.getFilePointer();

            writeStrToBuffer(shardFile, event.getApp());
            writeStrToBuffer(shardFile, event.getName());

            for (String property : CurrentHourPersistInfoRegister.getInstance().getEventLogSchema(logType).getBodyProperties()) {
                Object value = properties.get(property);
                if (value instanceof List) {
                    value = String.join(",", (List) value);
                }
                byte[] fb = BytesEncoderDecoder.encodeObj(value);
                shardFile.write(BytesEncoderDecoder.encode16(fb.length));
                shardFile.write(fb);
            }
            // add double value
            shardFile.write(BytesEncoderDecoder.encode64(event.value()));

            // add check code
            long currentSize = shardFile.getFilePointer() - pos;
            long checkCodelength = 8 - (currentSize % 8);
            shardFile.write(getCheckCode(checkCodelength));

            long endPos = shardFile.getFilePointer();
            shardFile.seek(pos);
            shardFile.write(BytesEncoderDecoder.encode16(endPos - pos - checkCodelength));
            shardFile.write(BytesEncoderDecoder.encode16(jumpperEndPos - pos - 4));
            shardFile.seek(endPos);

            // no need to flush, we now have check code
//            shardFile.flush();
//            shardFile.getChannel().force(false);

            return pos;
        } catch (IOException e) {
            logger.error("failed to log event", e);
            return -1;
        }
    }

    private byte[] getCheckCode(Long length) {
        byte[] bytes = new byte[length.shortValue()];
        Arrays.fill(bytes, ConstantsUtil.CHECK_CODE);
        return bytes;
    }

    private static void writeStrToBuffer(BufferedRandomAccessFile file, String s) throws IOException {
        if (s == null) {
            file.write(BytesEncoderDecoder.encode16(0));
            return;
        }
        byte[] bytes = s.getBytes(DEFAULT_CHARSET);
        file.write(BytesEncoderDecoder.encode16(bytes.length)); // key size
        file.write(bytes); // key
    }

}

