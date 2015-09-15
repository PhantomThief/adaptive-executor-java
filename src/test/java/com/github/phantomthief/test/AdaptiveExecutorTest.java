/**
 * 
 */
package com.github.phantomthief.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.github.phantomthief.concurrent.AdaptiveExecutor;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 */
public class AdaptiveExecutorTest {

    @Test
    public void test() {
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder() //
                .withGlobalMaxThread(10) //
                .adaptiveThread(5, 8) //
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                List<Integer> tasks = IntStream.range(0, new Random().nextInt(100) + 1).boxed()
                        .collect(Collectors.toList());
                Map<Integer, String> result = executor.invokeAll(tasks, this::exec);
                for (Integer task : tasks) {
                    assert((task + "").equals(result.get(task)));
                }
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS);
    }

    @Test
    public void test2() {
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder() //
                .withGlobalMaxThread(10) //
                .maxThreadAsPossible(5, 8) //
                .threadFactory(Executors.defaultThreadFactory()) //
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                List<Integer> tasks = IntStream.range(0, new Random().nextInt(100) + 1).boxed()
                        .collect(Collectors.toList());
                Map<Integer, String> result = new ConcurrentHashMap<>();
                executor.run(tasks, task -> result.put(task, task + ""));
                for (Integer task : tasks) {
                    assert((task + "").equals(result.get(task)));
                }
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS);
    }

    @Test
    public void test3() {
        AdaptiveExecutor.getCpuCoreAdpativeExecutor().invokeAll(null);
        AdaptiveExecutor.getCpuCoreAdpativeExecutor().invokeAll(Collections.emptyList());
    }

    @Test
    public void test4() {
        AdaptiveExecutor.getCpuCoreAdpativeExecutor().run(Collections.singleton(1),
                this::exception);
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder() //
                .withGlobalMaxThread(10) //
                .withThreadStrategy(i -> i) //)
                .build();
        List<Integer> tasks = Arrays.asList(1, 2, 3);
        Map<Integer, String> result = executor.invokeAll(tasks, this::exception);
        for (Integer task : tasks) {
            assert(result.containsKey(task));
            assert(result.get(task) == null);
        }
    }

    @Test
    public void testFastFail() {
        try {
            AdaptiveExecutor.newBuilder().build();
        } catch (NullPointerException e) {}
        try {
            AdaptiveExecutor.newBuilder().withGlobalMaxThread(1).build();
        } catch (NullPointerException e) {}
        try {
            AdaptiveExecutor.newBuilder().withGlobalMaxThread(1).withThreadStrategy(i -> 1).build();
        } catch (NullPointerException e) {}
    }

    private String exec(Integer i) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            //
        }
        return i + "";
    }

    private String exception(Integer i) {
        throw new RuntimeException();
    }

}
