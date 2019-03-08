package com.threathunter.persistent.core.api;

import com.threathunter.model.Event;
import com.threathunter.persistent.core.HourDirSequeceRead;
import com.threathunter.persistent.core.util.PathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yy on 17-11-3.
 */
public class SequenceReadContext {

    private static Logger logger = LoggerFactory.getLogger(LogsReadContext.class);
    //    private List<Event> events;
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
    private Visitor visitor;
    private Long startPoint;
    private Long endPoint;
    private String dir;

    public SequenceReadContext(Long startPoint, Long endPoint, String dir, Visitor visitor) {
        this.startPoint = startPoint;
        this.endPoint = endPoint;
        this.dir = dir;
        this.visitor = visitor;
//        events=new ArrayList<>();
    }

    public SequenceReadContext(String dir, Visitor visitor) {
        startPoint = getStartPointFromDir(dir);
        endPoint = getEndPointFromDir(dir);
        this.dir = dir;
        this.visitor = visitor;
    }

    private Long getEndPointFromDir(String dir) {
        return getStartPointFromDir(dir) + 60 * 60 * 1000L;
    }

    private Long getStartPointFromDir(String dir) {
        try {
            Date parsedDate = formatter.parse(dir);
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            return timestamp.getTime();
        } catch (Exception e) { //this generic but you can control another types of exception
            // look the origin of excption
            logger.error("dir name error");
            throw new RuntimeException("the dir not exist or the name is not valid");
        }
    }


    public Long getStartPoint() {
        return startPoint;
    }

    public void setStartPoint(Long startPoint) {
        this.startPoint = startPoint;
    }

    public Long getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Long endPoint) {
        this.endPoint = endPoint;
    }


    public void addEvent(Event event) {
        visitor.visit(event);
    }


    public void startQuery() {
     //   visitor.start();
        logger.info("query start in {}.", dir);
        if (PathHelper.ifDirExist(dir)) {
            HourDirSequeceRead query = new HourDirSequeceRead(this);
            query.read();
        }

    }

    public void endQuery() {
        logger.info("query end.");
//        writer.close();
   //     visitor.stop();
    }

    public String getDir() {
        return dir;
    }
}
