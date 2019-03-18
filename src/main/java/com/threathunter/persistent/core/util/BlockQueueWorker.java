package com.threathunter.persistent.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 
 */
public abstract class BlockQueueWorker<E> extends Thread {
    private final BlockingQueue<E> cache;
    private volatile boolean running = false;

    public BlockQueueWorker(BlockingQueue<E> eventBlockingQueue, String name, boolean daemon) {
        super(name);
        this.cache = eventBlockingQueue;
        this.setDaemon(daemon);
    }

    public void stopWorker() {
        if (!running) {
            return;
        }

        running = false;
        try {
            this.join();
        } catch (InterruptedException e) {
            dealWithInterruptedException(e);
        }
    }

    @Override
    public void run() {
        int idle = 0;
        while (running) {
            List<E> events = new ArrayList<>();
            cache.drainTo(events);
            if (events.isEmpty()) {
                idle++;
                if (idle >= 3) {
                    try {
                        Thread.sleep(100);
                        doIdleTask();
                    } catch (Exception e) {
                    }
                }
            } else {
                idle = 0;
                try {
                    doEventTask(events);
                } catch (Exception e) {
                }
            }
        }

        if (cache.size() > 0) {
            try {
                List<E> events = new ArrayList<>();
                cache.drainTo(events);
                doEventTask(events);
            } catch (Exception e) {
            }
        }

        cleanTask();
    }

    protected abstract void doIdleTask();

    protected abstract void doEventTask(List<E> events);

    protected abstract void cleanTask();

    protected abstract void dealWithInterruptedException(InterruptedException e);
}
