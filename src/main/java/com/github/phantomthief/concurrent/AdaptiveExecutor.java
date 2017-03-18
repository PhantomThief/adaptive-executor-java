/**
 *
 */
package com.github.phantomthief.concurrent;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;

import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class AdaptiveExecutor implements AutoCloseable {

    private static final long DEFAULT_TIMEOUT = SECONDS.toMillis(60);
    private static final Object EMPTY_OBJECT = new Object();
    private static final CallerRunsPolicy CALLER_RUNS_POLICY = new CallerRunsPolicy();
    private static final ListeningExecutorService DIRECT_EXECUTOR_SERVICE = newDirectExecutorService();
    private static Logger logger = getLogger(AdaptiveExecutor.class);
    private static CloseableSupplier<AdaptiveExecutor> cpuCoreAdaptive = lazy(
            AdaptiveExecutor.newBuilder() //
                    .withGlobalMaxThread(Runtime.getRuntime().availableProcessors()) //
                    .maxThreadAsPossible(Runtime.getRuntime().availableProcessors())::build);
    private final CloseableSupplier<ThreadPoolExecutor> threadPoolExecutor;
    private final IntUnaryOperator threadCountFunction;

    private AdaptiveExecutor(int globalMaxThread, long threadTimeout,
            IntUnaryOperator threadCountFunction, ThreadFactory threadFactory) {
        this.threadCountFunction = threadCountFunction;
        this.threadPoolExecutor = lazy(
                () -> new ThreadPoolExecutor(0, globalMaxThread, threadTimeout, MILLISECONDS,
                        new SynchronousQueue<>(), threadFactory, CALLER_RUNS_POLICY));
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static AdaptiveExecutor getCpuCoreAdpativeExecutor() {
        return cpuCoreAdaptive.get();
    }

    public final <K> void run(Collection<K> keys, Consumer<K> func) {
        run(null, keys, func);
    }

    public final <K> void run(String threadSuffixName, Collection<K> keys, Consumer<K> func) {
        invokeAll(threadSuffixName, keys, i -> {
            func.accept(i);
            return EMPTY_OBJECT;
        });
    }

    public final <K, V> Map<K, V> invokeAll(Collection<K> keys, Function<K, V> func) {
        return invokeAll(null, keys, func);
    }

    public final <K, V> Map<K, V> invokeAll(String threadSuffixName, Collection<K> keys,
            Function<K, V> func) {
        List<Callable<V>> calls = keys.stream().<Callable<V>> map(k -> () -> func.apply(k))
                .collect(toList());
        List<V> callResult = invokeAll(threadSuffixName, calls);
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
        return invokeAll(null, calls);
    }

    public final <V> List<V> invokeAll(String threadSuffixName, List<Callable<V>> calls) {
        if (calls == null || calls.isEmpty()) {
            return emptyList();
        }
        ExecutorService executorService;
        int thread = max(1, threadCountFunction.applyAsInt(calls.size()));
        if (thread == 1) {
            executorService = DIRECT_EXECUTOR_SERVICE;
        } else {
            executorService = threadPoolExecutor.get();
        }
        Thread callersThread = currentThread();
        List<Callable<List<V>>> packed = new ArrayList<>();
        for (List<Callable<V>> list : partition(calls,
                (int) ceil((double) calls.size() / thread))) {
            packed.add(() -> {
                String origThreadName = null;
                Thread runningThread = currentThread();
                if (runningThread != callersThread) {
                    origThreadName = renameCurrentThread(threadSuffixName);
                }
                try {
                    List<V> result = new ArrayList<>(list.size());
                    for (Callable<V> callable : list) {
                        result.add(callable.call());
                    }
                    return result;
                } finally {
                    if (origThreadName != null) {
                        runningThread.setName(origThreadName);
                    }
                }
            });
        }

        try {
            List<Future<List<V>>> invokeAll = executorService.invokeAll(packed);
            return invokeAll.stream().flatMap(this::futureGet).collect(toList());
        } catch (Throwable e) {
            logger.error("Ops.", e);
            return emptyList();
        }
    }

    private String renameCurrentThread(String threadNameSuffix) {
        Thread currentThread = currentThread();
        String originalName = currentThread.getName();
        currentThread.setName(originalName + "-" + threadNameSuffix);
        return originalName;
    }

    private <V> Stream<V> futureGet(Future<List<V>> future) {
        try {
            return future.get().stream();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public int getActiveCount() {
        return threadPoolExecutor.map(ThreadPoolExecutor::getActiveCount).orElse(-1);
    }

    public int getLargestPoolSize() {
        return threadPoolExecutor.map(ThreadPoolExecutor::getLargestPoolSize).orElse(-1);
    }

    @Override
    public void close() {
        threadPoolExecutor.tryClose(exec -> shutdownAndAwaitTermination(exec, 1, DAYS));
    }

    /**
     *
     */
    public static final class Builder {

        private int globalMaxThread;
        private IntUnaryOperator threadCountFunction;
        private ThreadFactory threadFactory;
        private long threadTimeout;

        public Builder withGlobalMaxThread(int globalMaxThread) {
            this.globalMaxThread = globalMaxThread;
            return this;
        }

        public Builder withThreadStrategy(IntUnaryOperator func) {
            this.threadCountFunction = func;
            return this;
        }

        public Builder threadTimeout(long time, TimeUnit unit) {
            this.threadTimeout = unit.toMillis(time);
            return this;
        }

        /**
         * @param maxThreadPerOp 每个操作最多的线程数，尽可能多的使用多线程
         */
        public Builder maxThreadAsPossible(int maxThreadPerOp) {
            this.threadCountFunction = i -> min(maxThreadPerOp, i);
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * @param minMultiThreadThreshold 操作数超过这个阈值就启用多线程
         * @param maxThreadPerOp 每个操作最多的线程数，尽可能多的使用多线程
         */
        public Builder maxThreadAsPossible(int minMultiThreadThreshold, int maxThreadPerOp) {
            this.threadCountFunction = i -> i <= minMultiThreadThreshold ? 1 : min(maxThreadPerOp,
                    i);
            return this;
        }

        /**
         * @param opPerThread 1个线程使用n个操作
         * @param maxThreadPerOp 单次操作最多线程数
         */
        public Builder adaptiveThread(int opPerThread, int maxThreadPerOp) {
            this.threadCountFunction = i -> min(maxThreadPerOp, i / opPerThread);
            return this;
        }

        public AdaptiveExecutor build() {
            ensure();
            return new AdaptiveExecutor(globalMaxThread, threadTimeout, threadCountFunction,
                    threadFactory);
        }

        private void ensure() {
            checkNotNull(threadCountFunction, "thread count function is null.");
            checkArgument(globalMaxThread > 0, "global max thread is illeagl.");

            if (threadTimeout <= 0) {
                threadTimeout = DEFAULT_TIMEOUT;
            }
            if (threadFactory == null) {
                threadFactory = new ThreadFactoryBuilder() //
                        .setNameFormat("pool-adaptive-thread-%d") //
                        .setUncaughtExceptionHandler((t, e) -> logger.error("Ops.", e)) //
                        .build();
            }
        }
    }
}
