package com.threathunter.persistent.core.api;

import com.threathunter.model.Event;
import com.threathunter.persistent.core.io.EventOfflineWriter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 */
public class LogsCreateContext {
    private  final EventOfflineWriter writer;
    private static AtomicInteger count=new AtomicInteger(0);
    public LogsCreateContext(){
        writer=EventOfflineWriter.getInstance();
    }
    public void startCreate(){
        writer.start();
        count.incrementAndGet();
    }

    public void stopCreate(){
        if(count.decrementAndGet()<=0)
        writer.stop();
    }

    public void addLog(Event event){
        writer.addLog(event);
    }
}
