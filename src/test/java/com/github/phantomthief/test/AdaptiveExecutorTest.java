/**
 * 
 */
package com.github.phantomthief.test;

import java.util.List;
import java.util.Map;
import java.util.Random;
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

    private String exec(Integer i) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            //
        }
        return i + "";
    }

}
