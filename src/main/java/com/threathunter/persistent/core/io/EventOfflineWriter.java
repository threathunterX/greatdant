package com.threathunter.persistent.core.io;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.persistent.core.CurrentHourPersistInfoRegister;
import com.threathunter.persistent.core.EventPersistCommon;
import com.threathunter.persistent.core.EventSchemaRegister;
import com.threathunter.persistent.core.util.ConstantsUtil;
import com.threathunter.persistent.core.util.MetricsHelper;
import com.threathunter.persistent.core.util.PathHelper;
import com.threathunter.persistent.core.util.SystemClock;
import com.google.common.hash.Hashing;
import org.apache.commons.net.util.SubnetUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.threathunter.persistent.core.EventPersistCommon.ensure_dir;


/**
 * Created by daisy on 16-4-7.
 *
 * Maintain a cache to write event record and index to files.
 */
public class EventOfflineWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventOfflineWriter.class);
    private static final EventOfflineWriter INSTANCE = new EventOfflineWriter();

    private final String persistentDir;
    private final long splitTimeIntervalMillis;
    private long currentSplitTimeMillis = -1;
    private long currentHalfMinuteTimeMills = -1;

    private final int eventLogShardCount;
    private final EventWriter eventWriter;
    private final LevelDbIndexReadWriter indexWriter;

    private final BlockingQueue<Event> cache = new LinkedBlockingDeque<>();
    private final Worker worker = new Worker();
    private volatile boolean running = false;

    // filtering data
    private List<String> suffixes = new ArrayList<>();
    private List<String> hosts = new ArrayList<>();
    private List<SubnetUtils.SubnetInfo> clients = new ArrayList<>();
    private List<SubnetUtils.SubnetInfo> servers = new ArrayList<>();
    private long updateTs = 0;

    private EventOfflineWriter() {
        this.persistentDir = CommonDynamicConfig.getInstance().getString(ConstantsUtil.PERSISTENT_DIR_CONFIG_NAME,
                PathHelper.getModulePath() + "/" + ConstantsUtil.PERSISTENT_DIR);
        ensure_dir(this.persistentDir);
        this.splitTimeIntervalMillis = ConstantsUtil.HOUR_MILLIS;
        this.eventLogShardCount = CommonDynamicConfig.getInstance().getInt(ConstantsUtil.PERSISTENT_SHARD_CONFIG_NAME, 16);

        this.eventWriter = new EventWriter();
        this.indexWriter = LevelDbIndexReadWriter.getInstance();
    }

    /**
     * Unit test will be error when using just one INSTANCE.
     * For the {@code worker} will stop and restart.
     * @return get new INSTANCE, will delete later
     */
    public static EventOfflineWriter newInstance() {
        return new EventOfflineWriter();
    }

    public static EventOfflineWriter getInstance() {
        return INSTANCE;
    }

    /**
     * Flush the index and logs into disk, be careful to call this method.
     * In the case of online, we need not, query data will be less but this is tolerant.
     */
    private void flush() {
        this.indexWriter.flushCache();
        this.eventWriter.flushAll();
    }

    public void addLog(final Event event)
    {
        if (!running) {
            return;
        }

        if (!this.cache.offer(event)) {
            MetricsHelper.getInstance().addMetrics("persist.events.drop.count", 1.0, "name", event.getName());
        } else {
            if (event.getName() != null) {
                MetricsHelper.getInstance().addMetrics("persist.events.offer.count", 1.0, "name", event.getName());
            }
        }
    }

    public void start() {
        if (running) {
            return;
        }

        this.currentSplitTimeMillis = SystemClock.getCurrentTimestamp() / splitTimeIntervalMillis * splitTimeIntervalMillis;
        String hourDir = String.format("%s/%s", this.persistentDir, new DateTime(this.currentSplitTimeMillis).toString("yyyyMMddHH"));
        ensure_dir(hourDir);

        try {
            writeAndUpdateCurrentSchemaAndKeys(hourDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.eventWriter.initial(hourDir, this.eventLogShardCount);
        this.indexWriter.initial(hourDir);

        running = true;
        this.worker.start();
    }

    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        try {
            worker.join();
            indexWriter.close();
            LOGGER.info("close index writer success");
            eventWriter.close();
            LOGGER.info("close event writer success");

            LOGGER.info("event offline LOGGER stop success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private final AtomicLong currentHourWriteCount = new AtomicLong(0);

    private class Worker extends Thread {
        public Worker() {
            super("event LOGGER");
            this.setDaemon(true);
        }

        @Override
        public void run() {
            int idle = 0;
            while (running) {
                List<Event> events = new ArrayList<>();
                cache.drainTo(events);

                if (events.isEmpty()) {
                    idle++;
                    if (idle >= 3) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception e) {
                            LOGGER.error("error in writing offline events", e);
                        }
                    }
                } else {
                    idle = 0;
                    for (Event event : events) {
                        try {
                            // no retry currently
                            logEvent(event);
                        } catch (Exception e) {
                            LOGGER.error("error in writing offline events", e);
                            MetricsHelper.getInstance().addMetrics("persist.events.log.error.count", 1.0, "name", event.getName(), "error", e.getClass().getSimpleName());
                        }
                    }
                }
            }

            if (cache.size() > 0) {
                List<Event> events = new ArrayList<>();
                cache.drainTo(events);
                // add to map for queue
                for (Event event : events) {
                    try {
                        logEvent(event);
                    } catch (Exception e) {
                        LOGGER.error("error in writing remaining offline events", e);
                        MetricsHelper.getInstance().addMetrics("persist.events.log.error.count", 1.0, "error", e.getClass().getSimpleName());
                    }
                }
            }
        }
    }

    private void logEvent(final Event e) throws UnknownHostException {
        long currentEventTimestamp = e.getTimestamp();
        long currentEventSplitTimeMillis = currentEventTimestamp / this.splitTimeIntervalMillis * this.splitTimeIntervalMillis;

        if (currentEventSplitTimeMillis < this.currentSplitTimeMillis) {
            return;
        }
        if (currentEventSplitTimeMillis > this.currentSplitTimeMillis) {
            if (!changeSplitToNextHour(currentEventSplitTimeMillis)) {
                return;
            }
            this.currentSplitTimeMillis = currentEventSplitTimeMillis;
            this.currentHourWriteCount.set(0);
        }
        if (e.getName() == null || e.getName().isEmpty()) {
            long currentHalfMinute = e.getTimestamp() / 30000 * 30000;
            if (currentHalfMinute > this.currentHalfMinuteTimeMills) {
                this.currentHalfMinuteTimeMills = currentHalfMinute;
                LOGGER.warn("flush log and index");
                flush();
            }
            return;
        }
        // do logging

        if (needBeFiltered(e)) {
            MetricsHelper.getInstance().addMetrics("persist.events.filter.count", 1.0, "name", e.getName());
            return;
        }

        int shard = getEventLogShard(e);

        long offset = eventWriter.writeEventLog(e, shard);
        if (offset >= 0) {
            Map<String, Object> properties = e.getPropertyValues();
            for (String headerField : CurrentHourPersistInfoRegister.getInstance().getCurrentEventHeaderKeys()) {
                byte[] bKey = EventPersistCommon.getIndexKeyBytes(headerField, (String) properties.get(headerField));

                if (indexWriter.writeIndex(bKey, offset, shard, e.getTimestamp())) {
                    MetricsHelper.getInstance().addMetrics("persist.events.index.count", 1.0, "header", headerField);
                } else {
                    MetricsHelper.getInstance().addMetrics("persist.events.index.error.count", 1.0, "header", headerField);
                }
            }
            currentHourWriteCount.incrementAndGet();
            MetricsHelper.getInstance().addMetrics("persist.events.log.count", 1.0, "name", e.getName(), "shard", "" + shard);
        } else {
            MetricsHelper.getInstance().addMetrics("persist.events.log.failed.count", 1.0, "name", e.getName(), "shard", "" + shard);
        }
    }

    private boolean changeSplitToNextHour(long newHourInMillis) {
        String newDir = String.format("%s/%s", this.persistentDir, new DateTime(newHourInMillis).toString("yyyyMMddHH"));
        ensure_dir(newDir);

        try {
            writeAndUpdateCurrentSchemaAndKeys(newDir);
        } catch (Exception e) {
            LOGGER.error("interrupted when writing schema and header file");
            return false;
        }
        return (this.eventWriter.updateLogPath(newDir) && this.indexWriter.updateIndexDir(newDir));
    }

    private void writeAndUpdateCurrentSchemaAndKeys(String baseDir) {
        try {
            File schemaFile = new File(String.format("%s/%s", baseDir, "events_schema.json"));
            File headerFile = new File(String.format("%s/%s", baseDir, "header_version.json"));
            if (!schemaFile.exists() && !headerFile.exists()) {
                if (EventSchemaRegister.getInstance().getUpdateTimeStamp() > 0) {
                    CurrentHourPersistInfoRegister.getInstance().updateFromLogSchemaRegister();
                }
            } else {
                CurrentHourPersistInfoRegister.getInstance().update(schemaFile);
            }
            if (!schemaFile.exists()) {
                EventSchemaWriter.getInstance().writeObjectToFile(
                        CurrentHourPersistInfoRegister.getInstance().getEventMetas(), schemaFile);
            }
            if (!headerFile.exists()) {
                EventSchemaWriter.getInstance().writeObjectToFile(
                        CurrentHourPersistInfoRegister.getInstance().getRawVersionHeaderMap(), headerFile);
            }
        } catch (Exception e) {
            LOGGER.error("fail to write schema file and version file");
        }
    }

    private final static char IP_DOT = '.';
    private int getEventLogShard(final Event event) {
        String ip = (String) event.getPropertyValues().get("c_ip");
        String key = "";
        if (ip != null) {
            int endIndex = ip.lastIndexOf(IP_DOT);
            if (endIndex > 0) {
                key = ip.substring(0, ip.lastIndexOf(IP_DOT));
            }
        }
        int shard = Hashing.murmur3_32().hashString(key, Charset.defaultCharset()).asInt() % this.eventLogShardCount;
        if (shard < 0) {
            shard *= -1;
        }
        return shard;
    }

    private void updateConfig() {
        long now = SystemClock.getCurrentTimestamp();
        if (now - this.updateTs < 30000) {
            return;
        }

        CommonDynamicConfig config = CommonDynamicConfig.getInstance();
        String hosts = config.getString("filter.log.domains", "");
        String clients = config.getString("filter.log.client_addresses", "");
        String servers = config.getString("filter.traffic.server_addresses", "");
        String suffixes = config.getString("filter.log.suffixes", "");

        this.hosts = new ArrayList<>(Arrays.asList(hosts.split(",")));
        ArrayList<SubnetUtils.SubnetInfo> newClients = new ArrayList<>();
        for (String ip : clients.split(",")) {
            try {
                if (ip.isEmpty()) {
                    continue;
                }
                SubnetUtils utils;
                if (ip.contains("/")) {
                    utils = new SubnetUtils(ip);
                } else {
                    utils = new SubnetUtils(String.format("%s/32", ip));
                }
                utils.setInclusiveHostCount(true);
                SubnetUtils.SubnetInfo info = utils.getInfo();
                newClients.add(info);
            } catch (Exception ignore) {
                LOGGER.warn("fail to get client filter on ip: {}", ip);
            }
        }
        this.clients = newClients;

        ArrayList<SubnetUtils.SubnetInfo> newServers = new ArrayList<>();
        for (String ip : servers.split(",")) {
            try {
                if (ip.isEmpty()) {
                    continue;
                }
                SubnetUtils utils;
                if (ip.contains("/")) {
                    utils = new SubnetUtils(ip);
                } else {
                    utils = new SubnetUtils(String.format("%s/32", ip));
                }
                utils.setInclusiveHostCount(true);
                SubnetUtils.SubnetInfo info = utils.getInfo();
                newServers.add(info);
            } catch (Exception ignore) {
                LOGGER.warn("fail to get client filter on ip: {}", ip);
            }
        }
        this.servers = newServers;
        this.suffixes = new ArrayList<>(Arrays.asList(suffixes.split(",")));

        this.updateTs = now;
    }

    private boolean needBeFiltered(Event e) {
        if (e.getName().equals("HTTP_STATIC")) {
            return true;
        }
        updateConfig();
        try {
            return needFilter(e);
        } catch (Exception ex) {
            LOGGER.error("fail to do filtering", ex);
        }
        return true;
    }

    private boolean needFilter(Event e) {
        String host = (String) e.getPropertyValues().get("host");
        if (host != null && !host.isEmpty()) {
            for (String s : hosts) {
                if (s == null || s.isEmpty())
                    continue;
                if (s.contains(host)) {
                    return true;
                }
            }
        }

        String uri = (String)e.getPropertyValues().get("uri_stem");
        if (uri != null && !uri.isEmpty()) {
            for (String s : suffixes) {
                if (s == null || s.isEmpty())
                    continue;
                if (uri.endsWith(s)) {
                    return true;
                }
            }
        }

        String cip = (String)e.getPropertyValues().get("c_ip");
        try {
            if (cip != null && !cip.isEmpty()) {
                int cipInt = getIntFromString(cip); // only used for sanity check
                for (SubnetUtils.SubnetInfo info : clients) {
                    // TODO this class can't accept integer
                    if (info.isInRange(cip)) {
                        return true;
                    }
                }
            }
        } catch(Exception ignore) {
        }

        String sip = (String)e.getPropertyValues().get("s_ip");
        try {
            if (sip != null && !sip.isEmpty()) {
                int sipInt = getIntFromString(sip); // only used for sanity check
                for (SubnetUtils.SubnetInfo info : servers) {
                    // TODO this class can't accept integer
                    if (info.isInRange(sip)) {
                        return true;
                    }
                }
            }
        } catch(Exception ignore) {
        }

        return false;
    }

    private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    private static final Pattern addressPattern = Pattern.compile(IP_ADDRESS);
    private static Integer getIntFromString(String address) {
        Matcher matcher = addressPattern.matcher(address);
        if (matcher.matches()) {
            int addr = 0;
            for (int i = 1; i <= 4; ++i) {
                int n = Integer.parseInt(matcher.group(i));
                addr |= ((n & 0xff) << 8*(4-i));
            }
            return addr;
        } else {
            throw new IllegalArgumentException("Could not parse [" + address + "]");
        }
    }
}