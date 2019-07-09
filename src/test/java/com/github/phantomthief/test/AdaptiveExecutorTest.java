package com.github.phantomthief.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.concurrent.AdaptiveExecutor;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 */
class AdaptiveExecutorTest {

    @Test
    void test() {
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder()
                .withGlobalMaxThread(10)
                .adaptiveThread(5, 8)
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                List<Integer> tasks = IntStream.range(0, new Random().nextInt(100) + 1).boxed()
                        .collect(Collectors.toList());
                Map<Integer, String> result = executor.invokeAll(tasks, this::exec);
                for (Integer task : tasks) {
                    assertEquals(task + "", result.get(task));
                }
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS);
    }

    @Test
    void test2() {
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder()
                .withGlobalMaxThread(10)
                .maxThreadAsPossible(5, 8)
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                List<Integer> tasks = IntStream.range(0, new Random().nextInt(100) + 1).boxed()
                        .collect(Collectors.toList());
                Map<Integer, String> result = new ConcurrentHashMap<>();
                executor.run(tasks, task -> result.put(task, task + ""));
                for (Integer task : tasks) {
                    assertEquals(task + "", result.get(task));
                }
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS);
    }

    @Test
    void test3() {
        AdaptiveExecutor.getCpuCoreAdpativeExecutor().invokeAll(null);
        AdaptiveExecutor.getCpuCoreAdpativeExecutor().invokeAll(Collections.emptyList());
    }

    @Test
    void test4() {
        AdaptiveExecutor.getCpuCoreAdpativeExecutor().run(Collections.singleton(1),
                this::exception);
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder()
                .withGlobalMaxThread(10)
                .withThreadStrategy(i -> i) //)
                .build();
        List<Integer> tasks = Arrays.asList(1, 2, 3);
        Map<Integer, String> result = executor.invokeAll(tasks, this::exception);
        for (Integer task : tasks) {
            assertTrue(result.containsKey(task));
            assertNull(result.get(task));
        }
    }

    @Test
    void testFastFail() {
        try {
            AdaptiveExecutor.newBuilder().build();
        } catch (NullPointerException e) {
            // do nothing
        }
        try {
            AdaptiveExecutor.newBuilder().withGlobalMaxThread(1).build();
        } catch (NullPointerException e) {
            // do nothing
        }
        try {
            AdaptiveExecutor.newBuilder().withGlobalMaxThread(1).withThreadStrategy(i -> 1).build();
        } catch (NullPointerException e) {
            // do nothing
        }
    }

    @Test
    void testName() {
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder()
                .withGlobalMaxThread(10)
                .adaptiveThread(5, 8)
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        Pattern pattern = Pattern.compile("mytest-\\d+-abc");
        for (int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                List<Integer> tasks = IntStream.range(0, new Random().nextInt(100) + 1).boxed()
                        .collect(Collectors.toList());
                Thread currentThread = Thread.currentThread();
                executor.run("abc", tasks, j -> {
                    Thread runningThread = Thread.currentThread();
                    if (currentThread != runningThread) {
                        assertTrue((pattern.matcher(Thread.currentThread().getName()).matches()));
                    }
                    exec(j);
                });
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS);
    }

    private String exec(Integer i) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {

        }
        return i + "";
    }

    private String exception(Integer i) {
        throw new RuntimeException();
    }
}
