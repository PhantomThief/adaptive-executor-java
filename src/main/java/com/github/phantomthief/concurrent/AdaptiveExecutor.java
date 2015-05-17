/**
 * 
 */
package com.github.phantomthief.concurrent;

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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 */
public class AdaptiveExecutor {

    private static final Object EMPTY_OBJECT = new Object();
    private static final CallerRunsPolicy CALLER_RUNS_POLICY = new CallerRunsPolicy();
    private static final ListeningExecutorService DIRECT_EXECUTOR_SERVICE = MoreExecutors
            .newDirectExecutorService();

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final int globalMaxThread;
    private final IntUnaryOperator threadCountFunction;

    private final AtomicInteger threadCounter = new AtomicInteger();

    /**
     * @param globalMaxThread
     * @param threadCountFunction
     */
    private AdaptiveExecutor(int globalMaxThread, IntUnaryOperator threadCountFunction) {
        this.globalMaxThread = globalMaxThread;
        this.threadCountFunction = threadCountFunction;
    }

    public final <K> void run(Collection<K> keys, Consumer<K> func) {
        invokeAll(keys, i -> {
            func.accept(i);
            return EMPTY_OBJECT;
        });
    }

    public final <K, V> Map<K, V> invokeAll(Collection<K> keys, Function<K, V> func) {
        List<Callable<V>> calls = keys.stream().<Callable<V>> map(k -> () -> func.apply(k))
                .collect(Collectors.toList());
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
        ExecutorService executorService = newExecutor(calls.size());
        try {
            List<Future<V>> invokeAll = executorService.invokeAll(calls);
            return invokeAll.stream().map(this::futureGet).collect(Collectors.toList());
        } catch (InterruptedException e) {
            logger.debug("Ops.{}", e.toString());
            return Collections.emptyList();
        } catch (Throwable e) {
            logger.error("Ops.", e);
            return Collections.emptyList();
        } finally {
            shutdownExecutor(executorService);
        }
    }

    private final int leftThreadCount(int old, int need) {
        if (old >= globalMaxThread) {
            return 0;
        }
        return Math.min(globalMaxThread - old, need);
    }

    private ExecutorService newExecutor(int keySize) {
        int needThread = threadCountFunction.applyAsInt(keySize);
        if (needThread <= 1) {
            return DIRECT_EXECUTOR_SERVICE;
        }
        int leftThread = threadCounter.updateAndGet(old -> leftThreadCount(old, needThread));
        if (leftThread <= 0) {
            return DIRECT_EXECUTOR_SERVICE;
        } else {
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(leftThread, leftThread,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1));
            threadPoolExecutor.setRejectedExecutionHandler(CALLER_RUNS_POLICY);
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
        if (MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.MINUTES)) {
            if (executorService instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
                threadCounter.addAndGet(-threadPoolExecutor.getCorePoolSize());
            }
        }
    }

    public static final class Builder {

        private int globalMaxThread;
        private IntUnaryOperator threadCountFunction;

        public Builder withGlobalMaxThread(int globalMaxThread) {
            this.globalMaxThread = globalMaxThread;
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

        /**
         * @param minMultiThreadThreshold 操作数超过这个阈值就启用多线程
         * @param maxThreadPerOp 每个操作最多的线程数，尽可能多的使用多线程
         * @return
         */
        public Builder maxThreadAsPossible(int minMultiThreadThreshold, int maxThreadPerOp) {
            this.threadCountFunction = i -> i <= minMultiThreadThreshold ? 1
                    : Math.min(maxThreadPerOp, i);
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
            return new AdaptiveExecutor(globalMaxThread, threadCountFunction);
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

}
