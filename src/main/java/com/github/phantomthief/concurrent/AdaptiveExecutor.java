/**
 * 
 */
package com.github.phantomthief.concurrent;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class AdaptiveExecutor implements Closeable {

    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(AdaptiveExecutor.class);

    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(60);
    private static final Object EMPTY_OBJECT = new Object();
    private static final CallerRunsPolicy CALLER_RUNS_POLICY = new CallerRunsPolicy();
    private static final ListeningExecutorService DIRECT_EXECUTOR_SERVICE = MoreExecutors
            .newDirectExecutorService();

    private final ThreadPoolExecutor threadPoolExecutor;
    private final IntUnaryOperator threadCountFunction;

    private AdaptiveExecutor(int globalMaxThread, //
            long threadTimeout, //
            IntUnaryOperator threadCountFunction, //
            ThreadFactory threadFactory) {
        this.threadCountFunction = threadCountFunction;
        this.threadPoolExecutor = new ThreadPoolExecutor(0, globalMaxThread, threadTimeout,
                TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), threadFactory,
                CALLER_RUNS_POLICY);
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
        ExecutorService executorService;
        int thread = Math.max(1, threadCountFunction.applyAsInt(calls.size()));
        if (thread == 1) {
            executorService = DIRECT_EXECUTOR_SERVICE;
        } else {
            executorService = threadPoolExecutor;
        }
        List<Callable<List<V>>> packed = new ArrayList<>();
        for (List<Callable<V>> list : Iterables.partition(calls,
                (int) Math.ceil((double) calls.size() / thread))) {
            packed.add(() -> {
                List<V> result = new ArrayList<>(list.size());
                for (Callable<V> callable : list) {
                    result.add(callable.call());
                }
                return result;
            });
        }

        try {
            List<Future<List<V>>> invokeAll = executorService.invokeAll(packed);
            return invokeAll.stream().flatMap(this::futureGet).collect(toList());
        } catch (Throwable e) {
            logger.error("Ops.", e);
            return Collections.emptyList();
        }
    }

    private final <V> Stream<V> futureGet(Future<List<V>> future) {
        try {
            return future.get().stream();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

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
            return new AdaptiveExecutor(globalMaxThread, threadTimeout, threadCountFunction,
                    threadFactory);
        }

        private void ensure() {
            Preconditions.checkNotNull(threadCountFunction, "thread count function is null.");
            Preconditions.checkArgument(globalMaxThread > 0, "global max thread is illeagl.");
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

    public int getActiveCount() {
        return threadPoolExecutor.getActiveCount();
    }

    public int getLargestPoolSize() {
        return threadPoolExecutor.getLargestPoolSize();
    }

    /* (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        MoreExecutors.shutdownAndAwaitTermination(threadPoolExecutor, 1, TimeUnit.DAYS);
    }
}
