/**
 * 
 */
package com.github.phantomthief.concurrent;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class AdaptiveExecutor {

    private static final Object EMPTY_OBJECT = new Object();
    private static final CallerRunsPolicy CALLER_RUNS_POLICY = new CallerRunsPolicy();
    private static final ListeningExecutorService DIRECT_EXECUTOR_SERVICE = MoreExecutors
            .newDirectExecutorService();

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final IntUnaryOperator threadCountFunction;
    private final ThreadFactory threadFactory;
    private final boolean callerRuns;

    private volatile int threadCounter;

    /**
     * @param globalMaxThread
     * @param threadCountFunction
     * @param threadFactory
     * @param callerRuns
     */
    private AdaptiveExecutor(int globalMaxThread, //
            IntUnaryOperator threadCountFunction, //
            ThreadFactory threadFactory, //
            boolean callerRuns) {
        this.threadCountFunction = threadCountFunction;
        this.threadFactory = threadFactory;
        this.callerRuns = callerRuns;
        this.threadCounter = globalMaxThread;
    }

    public final <K> void run(Collection<K> keys, Consumer<K> func) {
        invokeAll(keys, i -> {
            func.accept(i);
            return EMPTY_OBJECT;
        });
    }

    public final <K, V> Map<K, V> invokeAll(Collection<K> keys, Function<K, V> func) {
        List<Callable<V>> calls = keys.stream().<Callable<V>> map(k -> () -> func.apply(k))
                .collect(toList());
        List<V> callResult = invokeAll(calls);
        Iterator<V> iterator = callResult.iterator();
        Map<K, V> result = new HashMap<>();
        for (K key : keys) {
            V r;
            if (iterator.hasNext()) {
                r = iterator.next();
            } else {
                r = null;
            }
            result.put(key, r);
        }
        return result;
    }

    public final <V> List<V> invokeAll(List<Callable<V>> calls) {
        if (calls == null || calls.isEmpty()) {
            return Collections.emptyList();
        }
        ExecutorService executorService = newExecutor(calls.size(), callerRuns);
        try {
            List<Future<V>> invokeAll = executorService.invokeAll(calls);
            return invokeAll.stream().map(this::futureGet).collect(toList());
        } catch (Throwable e) {
            logger.error("Ops.", e);
            return Collections.emptyList();
        } finally {
            shutdownExecutor(executorService);
        }
    }

    private ExecutorService newExecutor(int keySize, boolean callerRuns) {
        int needThread = threadCountFunction.applyAsInt(keySize);
        if (needThread <= 1) {
            logger.trace("need thread one, using director service.");
            return DIRECT_EXECUTOR_SERVICE;
        }
        int leftThread;
        synchronized (this) {
            if (threadCounter >= needThread) {
                leftThread = needThread;
                threadCounter -= needThread;
            } else {
                leftThread = threadCounter;
                threadCounter = 0;
            }
        }
        if (leftThread <= 0) {
            logger.trace("no left thread availabled, using direct executor service.");
            return DIRECT_EXECUTOR_SERVICE;
        } else {
            ThreadPoolExecutor threadPoolExecutor;
            if (callerRuns) {
                threadPoolExecutor = new ThreadPoolExecutor(leftThread, leftThread, 0L,
                        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1), threadFactory,
                        CALLER_RUNS_POLICY);
            } else {
                threadPoolExecutor = new ThreadPoolExecutor(leftThread, leftThread, 0L,
                        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1) {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean offer(Runnable e) {
                                try {
                                    put(e);
                                    return true;
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                }
                                return false;
                            }
                        }, threadFactory);
            }
            logger.trace("init a executor, thread count:{}", leftThread);
            return threadPoolExecutor;
        }
    }

    private final <V> V futureGet(Future<V> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private final void shutdownExecutor(ExecutorService executorService) {
        if (executorService instanceof ListeningExecutorService) {
            return;
        }
        if (MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS)) {
            if (executorService instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
                synchronized (this) {
                    threadCounter += threadPoolExecutor.getCorePoolSize();
                    logger.trace("destoried a executor, with thread:{}, availabled thread:{}",
                            threadPoolExecutor.getCorePoolSize(), threadCounter);
                }
            }
        }
    }

    public static final class Builder {

        private int globalMaxThread;
        private IntUnaryOperator threadCountFunction;
        private ThreadFactory threadFactory;
        private boolean callerRuns;

        public Builder withGlobalMaxThread(int globalMaxThread) {
            this.globalMaxThread = globalMaxThread;
            return this;
        }

        public Builder enableCallerRunsPolicy() {
            callerRuns = true;
            return this;
        }

        public Builder withThreadStrategy(IntUnaryOperator func) {
            this.threadCountFunction = func;
            return this;
        }

        /**
         * @param maxThreadPerOp 每个操作最多的线程数，尽可能多的使用多线程
         * @return
         */
        public Builder maxThreadAsPossible(int maxThreadPerOp) {
            this.threadCountFunction = i -> Math.min(maxThreadPerOp, i);
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * @param minMultiThreadThreshold 操作数超过这个阈值就启用多线程
         * @param maxThreadPerOp 每个操作最多的线程数，尽可能多的使用多线程
         * @return
         */
        public Builder maxThreadAsPossible(int minMultiThreadThreshold, int maxThreadPerOp) {
            this.threadCountFunction = i -> i <= minMultiThreadThreshold ? 1 : Math
                    .min(maxThreadPerOp, i);
            return this;
        }

        /**
         * @param opPerThread 1个线程使用n个操作
         * @param maxThreadPerOp 单次操作最多线程数
         * @return
         */
        public Builder adaptiveThread(int opPerThread, int maxThreadPerOp) {
            this.threadCountFunction = i -> Math.min(maxThreadPerOp, i / opPerThread);
            return this;
        }

        public AdaptiveExecutor build() {
            ensure();
            return new AdaptiveExecutor(globalMaxThread, threadCountFunction, threadFactory,
                    callerRuns);
        }

        private void ensure() {
            Preconditions.checkNotNull(threadCountFunction, "thread count function is null.");
            Preconditions.checkArgument(globalMaxThread > 0, "global max thread is illeagl.");
            if (threadFactory == null) {
                threadFactory = new ThreadFactoryBuilder() //
                        .setNameFormat("pool-adaptive-thread-%d") //
                        .build();
            }
        }

    }

    public static final Builder newBuilder() {
        return new Builder();
    }

    private static Supplier<AdaptiveExecutor> cpuCoreAdaptive = Suppliers
            .memoize(AdaptiveExecutor.newBuilder() //
                    .withGlobalMaxThread(Runtime.getRuntime().availableProcessors()) //
                    .maxThreadAsPossible(Runtime.getRuntime().availableProcessors()) //
                    ::build);

    public static final AdaptiveExecutor getCpuCoreAdpativeExecutor() {
        return cpuCoreAdaptive.get();
    }

    public int getLeftThreadCount() {
        return threadCounter;
    }

}
