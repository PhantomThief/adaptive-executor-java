package com.github.phantomthief.concurrent;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

/**
 * @author w.vela
 * Created on 2017-09-12.
 */
public interface ExecutorFactory {

    ExecutorService create(int corePoolSize, int maxPoolThread, Duration keepAliveTime,
            BlockingQueue<Runnable> workQueue, CallerRunsPolicy policy);
}
