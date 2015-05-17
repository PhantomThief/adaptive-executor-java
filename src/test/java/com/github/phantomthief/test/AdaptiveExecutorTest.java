/**
 * 
 */
package com.github.phantomthief.test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.github.phantomthief.concurrent.AdaptiveExecutor;

/**
 * @author w.vela
 */
public class AdaptiveExecutorTest {

    @Test
    public void test() {
        List<Integer> tasks = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        AdaptiveExecutor executor = AdaptiveExecutor.newBuilder() //
                .withGlobalMaxThread(10) //
                .adaptiveThread(5, 8) //
                .build();
        Map<Integer, Integer> result = executor.invokeAll(tasks, Function.identity());
        tasks.forEach(i -> {
            assert(i.equals(result.get(i)));
        });
    }

}
